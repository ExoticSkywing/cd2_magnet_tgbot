# CloudDrive2 Telegram 下载管理器

**版本: 1.1.5**

项目简介：
这是一个专为 CloudDrive2 (CD2) 开发的 Telegram 机器人助手。它能够接收磁力链接、HTTP 链接及 ed2k 链接，并自动提交至 CD2 执行离线下载，同时提供强大的自动化后期清理功能。

---

## ✨ 功能特性

* 多协议支持：支持直接发送 magnet:?xt=、http://、https:// 以及 ed2k:// 链接进行离线下载。
* 智能后期清理：
    - **递归扫描**：深度扫描下载目录及所有子目录。
    - **小文件清理**：删除体积小于设定阈值（默认 300MB）的所有文件。
    - **黑名单过滤**：对大于阈值的文件，检查是否匹配黑名单关键词（如广告、.url、.txt 等），匹配则删除。
    - **空目录移除**：文件清理后，自动删除变为空的子目录。
* 网络代理支持：支持 http 和 socks5 代理，解决国内服务器无法连接 Telegram API 的问题。
* 自动命令菜单：机器人启动后会自动向 Telegram 注册 /clean 和 /blacklist 命令菜单。
* 安全保障：严格校验 ADMIN_IDS，仅限管理员操作。

---

## 🛠️ 部署指南 (Docker Compose)

推荐使用 Docker Compose 进行部署。仓库中的 `docker-compose.yml` 会从同目录的 `.env` 读取配置；真实 Token 不需要写入 Compose 文件。

```bash
cp -n .env.example .env
chmod 600 .env
# 编辑 .env，至少填写 CD2_ADDRESS、CD2_TOKEN、TG_TOKEN 和 ADMIN_IDS
docker compose up -d --build
docker compose logs -f cd2-bot
```

`.env` 已被 `.gitignore` 和 `.dockerignore` 排除，不会提交到 Git，也不会被复制进本地构建的镜像。

如果 CloudDrive2 以 Docker 的 `host` 网络模式运行，Linux 部署推荐将 `CD2_ADDRESS` 填为 `host.docker.internal:19798`；Compose 已自动配置 `host-gateway`。如果你的 CD2 只绑定在某个可达的局域网地址，也可以填该地址和端口。
---

## 📖 环境变量详细说明

| 变量名            | 必填 | 默认值 | 描述 |
|:---------------|:---| :--- | :--- |
| CD2_ADDRESS    | 是  | 127.0.0.1:19798 | CloudDrive2 的 IP 和 gRPC 端口 |
| CD2_TOKEN      | 是  | - | CloudDrive2 API 的 Access Token |
| TG_TOKEN       | 是  | - | Telegram Bot 的 API Token |
| ADMIN_IDS      | 是  | - | 允许使用机器人的用户数字 ID，逗号分隔 |
| SAVE_PATH      | 否  | /115/离线下载 | 离线下载任务存放的根路径 |
| SIZE_THRESHOLD | 否  | 300 | 文件体积小于此值(MB)将被删除，大于等于此值时检查黑名单 |
| PROXY_URL      | 否  | - | 连接 Telegram 的代理，支持 http/socks5 |
| NETWORK_ERROR_RESET_SECONDS | 否 | 300 | 网络异常静默达到此秒数后开始新一轮计数，仅用于日志诊断 |
| CLEAN_ENABLED  | 否  | true | 是否启用定时自动清理；设为 false 时仍可手动执行 /clean |
| CLEAN_CRON     | 否  |  30 3 * * * | 定时清理任务的 Cron 表达式|


---

## 🤖 指令说明

* 直接发送链接：发送磁力、HTTP 或 ed2k 链接，机器人自动提交下载任务。
* /clean：递归扫描目录，删除小文件和黑名单文件，清理空目录。
* /blacklist：查看当前已设置的黑名单关键词。
* /blacklist [关键词]：动态添加新的过滤关键词。

---

## 🛠️ 更新日志

### v1.1.5 (2026-07-15)
* **修复网络异常累计问题**：网络恢复后不再把历史错误永久累计到退出阈值
* **避免容器重启循环**：Telegram 或代理网络异常不再主动停止应用，交给内置轮询机制自动重连
* **新增异常窗口配置**：`NETWORK_ERROR_RESET_SECONDS` 默认 300 秒，仅用于分组记录诊断日志

### v1.1.4 (2026-06-08)
* **重构清理逻辑**：
    - 改为递归扫描所有子目录
    - 文件级删除判断：体积 < 阈值直接删除，体积 >= 阈值时检查黑名单
    - 清理文件后自动删除空目录
* **修复误删问题**：解决旧逻辑可能误删包含大文件的文件夹的问题

### v1.1.3 (2026-05-06)
* **新增网络重试次数限制**：添加 `MAX_RETRIES` 环境变量（默认10次），避免无限重试浪费资源
* **智能计数器重置**：网络恢复时自动重置重试计数器
* **修复启动崩溃**：移除不存在的 `run_polling` 参数（`retry_on_error` 等）
* **优化 CI 构建**：只在发布 tag 时构建镜像，同时生成版本号和 `latest` 标签

### v1.1.2 (2026-05-06)
* **修复代理配置**：为 Updater 补充 `get_updates_request` 代理配置，解决已读不回问题
* **增强错误日志**：对网络错误添加更详细的提示信息

### v1.1.1
* **修复代理配置**：为 Updater 补充 `get_updates_request` 代理配置，解决因 getUpdates 未走代理导致机器人无法收到指令（已读不回）的问题
* **适配 v22+ API**：解决配置 HTTPXRequest 时出现 'proxy_url' 意外参数的 TypeError

### v1.1.0
* **彻底解决假死问题**：改用 Telegram 原生 `JobQueue` 调度定时清理任务，避免 APScheduler 与 gRPC/Telegram 异步循环冲突

---

## 📝 开发者说明

项目基于 Python 开发，使用 gRPC 与 CloudDrive2 通信。
镜像构建通过 GitHub Actions 自动完成。

开源协议：MIT License
