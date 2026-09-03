# -*- coding: utf-8 -*-
"""
项目名称: CloudDrive2 Telegram 离线下载管家
版本: 1.1.5
功能描述:
    1. 链接监听: 自动识别 Magnet、HTTP、ed2k 链接并提交至 CD2 离线下载。
    2. 定时清理: 基于 Cron 表达式，递归扫描下载目录，删除小文件和黑名单文件，清理空目录。
    3. 异常容错: 增加全局错误处理与 gRPC 超时控制，防止网络波动导致假死。
作者: ymting
"""

import logging
import os
import re
import time
import grpc
import clouddrive_pb2
import clouddrive_pb2_grpc
from dataclasses import dataclass, field

# 版本号
__version__ = "1.1.5"
from apscheduler.triggers.cron import CronTrigger
from telegram import Update, BotCommand
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, CommandHandler, filters
from telegram.request import HTTPXRequest
from telegram.error import NetworkError, TimedOut

from app.config import IntegrationConfig
from app.javboss_client import JavBossRequestError
from app.models import JOB_AWAITING_SCAN, JOB_REJECTED
from app.runtime import IntegrationRuntime

# ==========================================
# 1. 变量配置区 (从 Docker 环境变量读取)
# ==========================================
CD2_IP_PORT = os.getenv("CD2_ADDRESS", "127.0.0.1:19798")  # CD2 的内网 IP 和 gRPC 端口
CD2_TOKEN = os.getenv("CD2_TOKEN", "")  # CD2 API 授权令牌
SAVE_PATH = os.getenv("SAVE_PATH", "/115/离线下载")  # 下载存放的根路径
TG_BOT_TOKEN = os.getenv("TG_TOKEN", "")  # Telegram 机器人 Token
ADMIN_IDS = [int(i) for i in os.getenv("ADMIN_IDS", "").split(",") if i.strip()]  # 允许操作的用户 ID
PROXY_URL = os.getenv("PROXY_URL", "")  # 连接 Telegram 的网络代理
CLEAN_CRON = os.getenv("CLEAN_CRON", "30 3 * * *")  # 定时清理的 Cron 表达式
CLEAN_ENABLED = os.getenv("CLEAN_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
BLACKLIST_FILE = "blacklist.txt"  # 黑名单关键词存储文件
SIZE_THRESHOLD_MB = int(os.getenv("SIZE_THRESHOLD", "300"))  # 有效文件的最小体积阈值
NETWORK_ERROR_RESET_SECONDS = int(os.getenv("NETWORK_ERROR_RESET_SECONDS", "300"))
if NETWORK_ERROR_RESET_SECONDS <= 0:
    raise ValueError("NETWORK_ERROR_RESET_SECONDS 必须是大于 0 的整数")
TELEGRAM_MESSAGE_LIMIT = 4096

# 配置日志输出，方便在 Docker 日志中查看运行状态
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# httpx 的默认 INFO 日志包含完整 Telegram Bot API URL，而 Token 位于 URL
# 路径中。生产日志绝不能记录该 URL。
logging.getLogger("httpx").setLevel(logging.WARNING)

INTEGRATION_CONFIG = IntegrationConfig.from_env()
JAV_STAGING_PATH = INTEGRATION_CONFIG.jav_staging_path
integration_runtime = IntegrationRuntime(INTEGRATION_CONFIG)

# 网络异常只按时间窗口分组记录，不再作为停止应用的条件
_network_error_count = 0
_last_network_error_at: float | None = None


# ==========================================
# 2. 核心清理逻辑
# ==========================================


@dataclass
class FolderCleanupResult:
    """一次任务文件夹清理的可展示统计。"""

    folder_name: str
    files_scanned: int = 0
    directories_scanned: int = 0
    files_deleted: int = 0
    small_files_deleted: int = 0
    blacklist_files_deleted: int = 0
    files_kept: int = 0
    folders_deleted: int = 0
    error: str = ""


@dataclass
class CleanupSummary:
    """一次 /clean 执行的总览统计。"""

    task_folders_scanned: int = 0
    directories_scanned: int = 0
    files_scanned: int = 0
    files_deleted: int = 0
    small_files_deleted: int = 0
    blacklist_files_deleted: int = 0
    files_kept: int = 0
    folders_deleted: int = 0
    errors: int = 0
    details: list[str] = field(default_factory=list)

    def add(self, result: FolderCleanupResult) -> None:
        self.task_folders_scanned += 1
        self.directories_scanned += result.directories_scanned
        self.files_scanned += result.files_scanned
        self.files_deleted += result.files_deleted
        self.small_files_deleted += result.small_files_deleted
        self.blacklist_files_deleted += result.blacklist_files_deleted
        self.files_kept += result.files_kept
        self.folders_deleted += result.folders_deleted
        if result.error:
            self.errors += 1

        if result.error:
            actions = []
            if result.files_deleted:
                actions.append(f"已删除文件 {result.files_deleted} 个")
            if result.folders_deleted:
                actions.append(f"已删除文件夹 {result.folders_deleted} 个")
            progress = f"（{'，'.join(actions)}）" if actions else ""
            self.details.append(
                f"❌ `{result.folder_name}`：处理失败{progress}：{result.error}"
            )
            return
        if result.files_deleted == 0 and result.folders_deleted == 0:
            return

        actions = []
        if result.files_deleted:
            actions.append(
                f"删除文件 {result.files_deleted} 个"
                f"（小文件 {result.small_files_deleted}，黑名单 {result.blacklist_files_deleted}）"
            )
        if result.folders_deleted:
            actions.append(f"删除文件夹 {result.folders_deleted} 个")
        self.details.append(
            f"• `{result.folder_name}`：扫描 {result.files_scanned} 个文件，"
            + "，".join(actions)
        )


def format_cleanup_report(
    summary: CleanupSummary, max_length: int = TELEGRAM_MESSAGE_LIMIT
) -> str:
    """按“总览在前、明细在后”生成 Telegram 清理报告。"""

    lines = [
        "📊 *JAV 待验收区清理报告*",
        "",
        f"任务文件夹：{summary.task_folders_scanned} 个",
        f"扫描目录：{summary.directories_scanned} 个",
        f"扫描文件：{summary.files_scanned} 个",
        f"保留文件：{summary.files_kept} 个",
        (
            f"删除文件：{summary.files_deleted} 个"
            f"（小文件 {summary.small_files_deleted}，黑名单 {summary.blacklist_files_deleted}）"
        ),
        f"删除文件夹：{summary.folders_deleted} 个",
        f"异常：{summary.errors} 个",
    ]
    if summary.details:
        lines.extend(["", "明细："])
        # Telegram 单条消息上限为 4096 字符；总览必须始终保留，明细按整行截断。
        omitted = 0
        for detail in summary.details:
            candidate = "\n".join([*lines, detail])
            if len(candidate) > max_length:
                omitted += 1
                continue
            lines.append(detail)
        if omitted:
            omitted_line = f"…其余 {omitted} 个目录明细已省略（总览统计仍完整）"
            if len("\n".join([*lines, omitted_line])) <= max_length:
                lines.append(omitted_line)
    elif summary.files_deleted == 0 and summary.folders_deleted == 0 and summary.errors == 0:
        lines.extend(["", "结果：无需清理。"])
    return "\n".join(lines)


def get_blacklist():
    """读取黑名单配置，若文件不存在则创建默认列表"""
    if not os.path.exists(BLACKLIST_FILE):
        default_list = ["广告", "promo", ".url", "txt", "readme", "扫码", "最新地址"]
        with open(BLACKLIST_FILE, "w", encoding="utf-8") as f:
            for k in default_list: f.write(f"{k}\n")
        return default_list
    with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


async def get_all_items_recursive(stub, metadata, folder_path) -> tuple[list, list]:
    """
    递归获取文件夹下所有文件和目录
    返回: (文件列表, 目录列表)
    """
    files = []
    directories = []

    req = clouddrive_pb2.ListSubFileRequest(path=folder_path)
    sub_items = []
    async for reply in stub.GetSubFiles(req, metadata=metadata, timeout=15):
        if reply.subFiles:
            sub_items.extend(reply.subFiles)

    for item in sub_items:
        if item.isDirectory:
            directories.append(item)
            # 递归获取子目录中的内容
            sub_files, sub_dirs = await get_all_items_recursive(stub, metadata, item.fullPathName)
            files.extend(sub_files)
            directories.extend(sub_dirs)
        else:
            files.append(item)

    return files, directories


async def is_directory_empty(stub, metadata, dir_path) -> bool:
    """检查目录是否为空"""
    req = clouddrive_pb2.ListSubFileRequest(path=dir_path)
    sub_items = []
    async for reply in stub.GetSubFiles(req, metadata=metadata, timeout=15):
        if reply.subFiles:
            sub_items.extend(reply.subFiles)
    return len(sub_items) == 0


async def delete_cloud_paths(stub, metadata, paths: list[str]) -> None:
    """删除指定云端路径，并确保 CloudDrive2 返回成功。"""

    if not paths:
        return
    result = await stub.DeleteFiles(
        clouddrive_pb2.MultiFileRequest(path=paths), metadata=metadata
    )
    if not result.success:
        raise RuntimeError(result.errorMessage or "CloudDrive2 删除操作失败")


async def clean_task_folder(stub, metadata, folder_path) -> FolderCleanupResult:
    """
    对单个任务文件夹执行清理动作:
    - 递归扫描所有文件
    - 体积 < 阈值的文件删除
    - 体积 >= 阈值且匹配黑名单的文件删除
    - 清理空目录（不删除 folder_path 本身）
    """
    folder_name = os.path.basename(folder_path)
    result = FolderCleanupResult(folder_name=folder_name)
    try:
        # 递归获取所有文件和目录
        all_files, all_dirs = await get_all_items_recursive(stub, metadata, folder_path)
        result.files_scanned = len(all_files)
        # all_dirs 不包含当前任务文件夹本身，因此这里加 1。
        result.directories_scanned = len(all_dirs) + 1
        logger.info(f"📁 扫描 `{folder_name}`: 发现 {len(all_files)} 个文件, {len(all_dirs)} 个目录")

        # 如果没有任何内容，直接删除空文件夹
        if not all_files and not all_dirs:
            await delete_cloud_paths(stub, metadata, [folder_path])
            result.folders_deleted = 1
            return result

        # 判断删除条件
        current_black = get_blacklist()
        threshold_bytes = SIZE_THRESHOLD_MB * 1024 * 1024
        files_to_delete = []
        small_files_to_delete = 0
        blacklist_files_to_delete = 0

        for f in all_files:
            size_mb = f.size / (1024 * 1024)
            if f.size < threshold_bytes:
                # 体积 < 阈值，删除
                logger.debug(f"  🗑️ 标记删除(小文件): {f.name} ({size_mb:.1f}MB)")
                files_to_delete.append(f.fullPathName)
                small_files_to_delete += 1
            elif any(k.lower() in f.name.lower() for k in current_black):
                # 体积 >= 阈值但匹配黑名单，删除
                logger.debug(f"  🗑️ 标记删除(黑名单): {f.name} ({size_mb:.1f}MB)")
                files_to_delete.append(f.fullPathName)
                blacklist_files_to_delete += 1
            else:
                logger.debug(f"  ✅ 保留: {f.name} ({size_mb:.1f}MB)")

        logger.info(f"  待删除文件数: {len(files_to_delete)}/{len(all_files)}")
        # 先按“当前仍保留”计算；删除接口成功后再把候选计入已删除。
        result.files_kept = len(all_files)

        # 执行文件删除
        if files_to_delete:
            await delete_cloud_paths(stub, metadata, files_to_delete)
            result.files_deleted = len(files_to_delete)
            result.small_files_deleted = small_files_to_delete
            result.blacklist_files_deleted = blacklist_files_to_delete
            result.files_kept -= result.files_deleted

        # 清理空目录（从最深层开始）
        all_dirs.sort(key=lambda x: x.fullPathName.count('/'), reverse=True)

        for d in all_dirs:
            if await is_directory_empty(stub, metadata, d.fullPathName):
                await delete_cloud_paths(stub, metadata, [d.fullPathName])
                result.folders_deleted += 1

        # 最后检查 folder_path 是否为空
        if await is_directory_empty(stub, metadata, folder_path):
            await delete_cloud_paths(stub, metadata, [folder_path])
            result.folders_deleted += 1
        return result

    except Exception as e:
        logger.error(f"处理文件夹 {folder_name} 出错: {str(e)}")
        result.error = str(e)
        return result


async def run_auto_clean():
    """只清理 JavBoss 待验收目录，绝不遍历云下载中的其它内容。"""
    logger.info("⏰ [Schedule] 开始清理 JAV 待验收目录: %s", JAV_STAGING_PATH)
    summary = CleanupSummary()
    try:
        async with grpc.aio.insecure_channel(CD2_IP_PORT) as channel:
            stub = clouddrive_pb2_grpc.CloudDriveFileSrvStub(channel)
            metadata = [('authorization', f'Bearer {CD2_TOKEN}')]
            root_req = clouddrive_pb2.ListSubFileRequest(path=JAV_STAGING_PATH)

            async for reply in stub.GetSubFiles(root_req, metadata=metadata, timeout=30):
                if reply.subFiles:
                    for f in reply.subFiles:
                        if f.isDirectory:
                            summary.add(await clean_task_folder(stub, metadata, f.fullPathName))
        logger.info(
            "✅ [Schedule] JAV 待验收目录清理完成：任务目录=%d 扫描目录=%d 扫描文件=%d 删除文件=%d 删除文件夹=%d 异常=%d",
            summary.task_folders_scanned,
            summary.directories_scanned,
            summary.files_scanned,
            summary.files_deleted,
            summary.folders_deleted,
            summary.errors,
        )
    except Exception as e:
        logger.error(f"❌ [Schedule] 自动任务运行失败: {str(e)}")


# ==========================================
# 3. Telegram 交互处理器
# ==========================================

def _is_network_error(error: object) -> bool:
    """识别 Telegram/httpx 抛出的可恢复网络异常。"""
    error_text = str(error)
    return (
        isinstance(error, (NetworkError, TimedOut))
        or "ConnectError" in error_text
        or "ConnectTimeout" in error_text
    )


def _record_network_error(now: float | None = None) -> int:
    """记录当前网络异常，并在静默超过配置窗口后开始新一轮计数。"""
    global _network_error_count, _last_network_error_at

    current_time = time.monotonic() if now is None else now
    # 成功请求不会进入错误处理器，因此在下一次异常到来时按静默时长惰性重置。
    if (
        _last_network_error_at is None
        or current_time - _last_network_error_at >= NETWORK_ERROR_RESET_SECONDS
    ):
        _network_error_count = 1
    else:
        _network_error_count += 1

    _last_network_error_at = current_time
    return _network_error_count

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """全局错误拦截器，可恢复网络异常交给 Telegram 轮询机制继续重连。"""
    error = context.error

    if _is_network_error(error):
        error_count = _record_network_error()
        logger.warning(
            "🌐 网络连接异常，本轮第 %d 次；连续 %d 秒无异常后重新计数。"
            "Telegram 轮询将继续自动重连。错误: %s",
            error_count,
            NETWORK_ERROR_RESET_SECONDS,
            error,
        )
        return

    logger.error("⚠️ 机器人运行时捕获到非网络异常: %s", error)


async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """监听并处理发送的磁力链接、HTTP、电驴链接"""
    if update.effective_user.id not in ADMIN_IDS: return
    text = update.message.text.strip()

    if any(text.startswith(p) for p in ["magnet:", "http", "ed2k://"]):
        try:
            async with grpc.aio.insecure_channel(CD2_IP_PORT) as channel:
                stub = clouddrive_pb2_grpc.CloudDriveFileSrvStub(channel)
                metadata = [('authorization', f'Bearer {CD2_TOKEN}')]
                req = clouddrive_pb2.AddOfflineFileRequest(urls=text, toFolder=SAVE_PATH)
                res = await stub.AddOfflineFiles(req, metadata=metadata, timeout=15)
                if res.success:
                    await update.message.reply_text(
                        f"✅ 提交成功！\n📂 目录：`{SAVE_PATH}`\n提示：完成后发送 /clean 执行清理。")
                else:
                    await update.message.reply_text(f"❌ CD2 拒绝请求: {res.errorMessage}")
        except Exception as e:
            await update.message.reply_text(f"❌ 提交失败，CD2 连接异常: {str(e)}")


def _jav_command_input(text: str) -> str:
    return re.sub(r"^/jav(?:@\w+)?(?:\s+|$)", "", text or "", count=1, flags=re.I).strip()


def _jav_input_summary(batch: dict) -> str:
    input_count = int(batch.get("input_count") or 0)
    parsed_count = int(batch.get("parsed_count") or 0)
    accepted_count = int(batch.get("accepted_count") or 0)
    existing_count = int(batch.get("library_duplicate_count") or 0) + int(
        batch.get("history_duplicate_count") or 0
    )
    duplicate_count = int(batch.get("batch_duplicate_count") or 0)
    invalid_count = int(batch.get("invalid_count") or 0)
    lines = [
        "✅ 已提交到 JavBoss",
        f"输入片段：{input_count}",
        f"识别番号：{parsed_count}",
        f"新增作品：{accepted_count}",
        f"已有作品：{existing_count}",
        f"批内重复：{duplicate_count}",
    ]
    if invalid_count:
        lines.append(f"需要留意：{invalid_count}")
    items = batch.get("items") if isinstance(batch.get("items"), list) else []
    accepted_codes = [
        str(item.get("code") or "").strip()
        for item in items
        if item.get("status") == "accepted" and str(item.get("code") or "").strip()
    ]
    if accepted_codes:
        preview = "、".join(accepted_codes[:20])
        if len(accepted_codes) > 20:
            preview += f" 等 {len(accepted_codes)} 部"
        lines.extend(("", f"新增：{preview}"))
    return "\n".join(lines)


async def _submit_jav_input(update: Update, raw_input: str) -> bool:
    if update.effective_user.id not in ADMIN_IDS:
        return False
    message = update.effective_message
    chat = update.effective_chat
    if message is None or chat is None:
        return False
    request_id = f"telegram:{chat.id}:{message.message_id}"
    status_message = await message.reply_text("🔎 正在交给 JavBoss 解析并去重…")
    try:
        batch = await integration_runtime.javboss.submit_jav_input(
            raw_input, idempotency_key=request_id
        )
    except JavBossRequestError as error:
        await status_message.edit_text(f"❌ JavBoss 番号输入失败：{error}")
        return False
    await status_message.edit_text(_jav_input_summary(batch))
    return True


async def cmd_jav(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """提交番号；无参数时让下一条普通文本成为完整原始输入。"""
    if update.effective_user.id not in ADMIN_IDS:
        return
    raw_input = _jav_command_input(update.effective_message.text or "")
    if raw_input:
        await _submit_jav_input(update, raw_input)
        return
    context.user_data["awaiting_jav_input"] = True
    await update.effective_message.reply_text(
        "请发送番号内容，可一次发送一个、多行或混杂文本。\n发送 /cancel 取消。"
    )


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if context.user_data.pop("awaiting_jav_input", None):
        await update.effective_message.reply_text("已取消番号输入。")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """按会话意图分流普通文本，链接入口保持原行为。"""
    if update.effective_user.id not in ADMIN_IDS:
        return
    if context.user_data.get("awaiting_jav_input"):
        raw_input = (update.effective_message.text or "").strip()
        if await _submit_jav_input(update, raw_input):
            context.user_data.pop("awaiting_jav_input", None)
        return
    await handle_link(update, context)


async def cmd_clean(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """手动清理 JavBoss 待验收目录 (/clean)。"""
    if update.effective_user.id not in ADMIN_IDS: return
    status_msg = await update.message.reply_text("🔍 正在全量扫描目录，请稍后...")
    summary = CleanupSummary()
    try:
        async with grpc.aio.insecure_channel(CD2_IP_PORT) as channel:
            stub = clouddrive_pb2_grpc.CloudDriveFileSrvStub(channel)
            metadata = [('authorization', f'Bearer {CD2_TOKEN}')]
            root_req = clouddrive_pb2.ListSubFileRequest(path=JAV_STAGING_PATH)
            async for reply in stub.GetSubFiles(root_req, metadata=metadata, timeout=30):
                if reply.subFiles:
                    for f in reply.subFiles:
                        if f.isDirectory:
                            summary.add(await clean_task_folder(stub, metadata, f.fullPathName))
            logger.info(
                "📂 JAV 待验收区清理统计：任务目录=%d 扫描目录=%d 扫描文件=%d 删除文件=%d 删除文件夹=%d 异常=%d",
                summary.task_folders_scanned,
                summary.directories_scanned,
                summary.files_scanned,
                summary.files_deleted,
                summary.folders_deleted,
                summary.errors,
            )

        await status_msg.edit_text(
            format_cleanup_report(summary), parse_mode='Markdown'
        )
    except Exception as e:
        await status_msg.edit_text(f"❌ 无法执行清理: `{str(e)}`")


async def cmd_blacklist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """管理黑名单关键词 (/blacklist)"""
    if update.effective_user.id not in ADMIN_IDS: return
    current = get_blacklist()
    if context.args:
        new_word = " ".join(context.args)
        if new_word not in current:
            current.append(new_word)
            with open(BLACKLIST_FILE, "w", encoding="utf-8") as f:
                for k in current: f.write(f"{k}\n")
            await update.message.reply_text(f"➕ 已添加黑名单关键词: `{new_word}`", parse_mode='Markdown')
    else:
            await update.message.reply_text(f"📝 当前黑名单:\n`{', '.join(current)}`", parse_mode='Markdown')


async def notify_jav_review_batch(bot, result) -> None:
    """向管理员广播一次 JavBoss 验收批次的最终汇总。"""

    jobs = list(getattr(result, "jobs", []) or [])
    approved = sum(1 for job in jobs if job.status == JOB_AWAITING_SCAN)
    rejected = sum(1 for job in jobs if job.status == JOB_REJECTED)
    cleanup = dict(getattr(result, "cleanup", {}) or {})
    lines = [
        "✅ JavBoss 批量验收已执行",
        f"通过：{approved} 部",
        f"不合格：{rejected} 部",
        (
            "执行前清扫："
            f"扫描 {int(cleanup.get('task_folders_scanned') or 0)} 个任务目录，"
            f"删除文件 {int(cleanup.get('files_deleted') or 0)} 个 "
            f"（小文件 {int(cleanup.get('small_files_deleted') or 0)}，"
            f"黑名单 {int(cleanup.get('blacklist_files_deleted') or 0)}），"
            f"删除空目录 {int(cleanup.get('folders_deleted') or 0)} 个"
        ),
    ]
    if approved:
        lines.append("通过作品已移入正式扫描目录，等待 JavBoss 扫盘确认。")
    if rejected:
        lines.append("不合格作品已从待验收目录删除，磁链记录仍保留。")
    message = "\n".join(lines)
    for chat_id in ADMIN_IDS:
        try:
            await bot.send_message(chat_id=chat_id, text=message)
        except Exception as error:  # Telegram 权限/网络问题不影响批次结果
            logger.warning("JavBoss 验收 Telegram 通知失败 chat_id=%s：%s", chat_id, error)


async def post_init(application):
    """
    机器人启动后的初始化:
    - 注册手机端指令菜单。
    - 在运行中的事件循环内启动 Cron 调度器，解决 RuntimeError 问题。
    """
    await application.bot.set_my_commands([
        BotCommand("jav", "向 JavBoss 输入一个或一批番号"),
        BotCommand("cancel", "取消当前番号输入"),
        BotCommand("clean", "清理 JAV 待验收目录"),
        BotCommand("blacklist", "查看或更新黑名单关键词")
    ])
    integration_runtime.http.set_review_notifier(
        lambda result: notify_jav_review_batch(application.bot, result)
    )
    await integration_runtime.start()
    if application.job_queue:
        application.job_queue.run_repeating(
            integration_runtime.poll_downloads_job,
            interval=INTEGRATION_CONFIG.poll_interval_seconds,
            first=5,
            name="jav-download-poll",
        )
        application.job_queue.run_repeating(
            integration_runtime.flush_callbacks_job,
            interval=INTEGRATION_CONFIG.callback_retry_seconds,
            first=10,
            name="jav-callback-outbox",
        )
    # 初始化并启动调度器
    # 修复假死问题：不要单独创建 AsyncIOScheduler 实例，否则会引发 asyncio 事件循环冲突
    # 改为使用 python-telegram-bot 内置的 job_queue，由于自带的 job_queue 可以良好管理协程，避免卡死。
    if not CLEAN_ENABLED:
        logger.info("🛑 定时自动清理已禁用（CLEAN_ENABLED=false），/clean 仍可手动执行。")
        return

    if application.job_queue:
        # job_queue 内部包含了一个配置好的 apscheduler 实例
        application.job_queue.scheduler.add_job(
            run_auto_clean, 
            CronTrigger.from_crontab(CLEAN_CRON)
        )
        logger.info(f"📅 定时任务系统已启动(基于内置JobQueue)，Cron 设定: [{CLEAN_CRON}]")
    else:
        logger.error("❌ 无法启动定时清理任务：内置的 JobQueue 未初始化。")


async def post_shutdown(_application):
    await integration_runtime.close()


# ==========================================
# 4. 程序入口
if __name__ == '__main__':
    # 代理网络配置
    request_kwargs = {
        "connection_pool_size": 8,
        "read_timeout": 30.0,
        "write_timeout": 30.0,
        "connect_timeout": 20.0,
        "pool_timeout": 15.0
    }
    
    if PROXY_URL:
        logger.info(f"正在配置网络代理: {PROXY_URL}")
        # telegram.request.HTTPXRequest 在 v22+ 支持直接传入 proxy 参数
        q_request = HTTPXRequest(proxy=PROXY_URL, **request_kwargs)
        u_request = HTTPXRequest(proxy=PROXY_URL, **request_kwargs)
    else:
        q_request = HTTPXRequest(**request_kwargs)
        u_request = HTTPXRequest(**request_kwargs)
        
    # 构造应用实例，并同时为 bot 实例和 updater(getUpdates轮询) 注入支持代理的网络请求类
    builder = (
        ApplicationBuilder()
        .token(TG_BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .request(q_request)
        .get_updates_request(u_request)
    )

    app = builder.build()

    # 注册异常拦截器
    app.add_error_handler(error_handler)

    # 注册消息与指令处理器
    app.add_handler(CommandHandler("jav", cmd_jav))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_text))
    app.add_handler(CommandHandler("clean", cmd_clean))
    app.add_handler(CommandHandler("blacklist", cmd_blacklist))

    logger.info("🚀 CD2 Bot 已启动，正在轮询消息...")
    # python-telegram-bot 的 run_polling 默认在遇到网络错误时会自动重试
    # 通过 error_handler 捕获并记录异常，无需额外配置重试参数
    app.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES,
    )
