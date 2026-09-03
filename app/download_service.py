from __future__ import annotations

import asyncio
import base64
import binascii
import copy
import json
import logging
import os
import posixpath
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from collections.abc import Awaitable, Callable
from urllib.parse import parse_qs, urlparse

import clouddrive_pb2

from .cd2_client import CloudDriveClient, CloudDriveError, OfflineTask
from .config import IntegrationConfig
from .jav_cleanup import clean_jav_paths, load_blacklist
from .javboss_client import JavBossClient
from .models import (
    JOB_AWAITING_QUALITY,
    JOB_AWAITING_SCAN,
    JOB_FAILED,
    JOB_RECEIVED,
    JOB_REJECTED,
    JOB_SUBMITTED,
    JOB_SUBMITTING,
    JOB_UNCERTAIN,
    DownloadJob,
    utc_now_text,
)
from .repository import IntegrationRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReviewBatchResult:
    jobs: list[DownloadJob]
    cleanup: dict[str, int]


def magnet_info_hash(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme.lower() != "magnet":
        return ""
    for value in parse_qs(parsed.query).get("xt", []):
        prefix, separator, raw_hash = value.partition(":btih:")
        if not separator or prefix.lower() != "urn":
            continue
        value = raw_hash.strip()
        if len(value) == 40:
            try:
                int(value, 16)
            except ValueError:
                continue
            return value.lower()
        if len(value) == 32:
            try:
                return base64.b32decode(value.upper()).hex()
            except (binascii.Error, ValueError):
                continue
    return ""


class DownloadGatewayService:
    def __init__(
        self,
        config: IntegrationConfig,
        repository: IntegrationRepository,
        cd2: CloudDriveClient,
        javboss: JavBossClient,
    ):
        self.config = config
        self.repository = repository
        self.cd2 = cd2
        self.javboss = javboss
        self._poll_lock = asyncio.Lock()
        self._status_notifier: Callable[[DownloadJob], Awaitable[None]] | None = None

    def set_status_notifier(
        self, notifier: Callable[[DownloadJob], Awaitable[None]] | None
    ) -> None:
        """Register an optional user-facing notification for durable status changes."""

        self._status_notifier = notifier

    async def check_ready(self) -> tuple[bool, str]:
        if not self.config.gateway_enabled:
            return False, "下载网关密钥或 CD2 Token 未配置"
        try:
            parent, name = posixpath.split(self.config.jav_staging_path)
            children = await self.cd2.list_children(parent)
        except Exception as error:
            return False, str(error)
        match = next((row for row in children if row.name == name), None)
        if match is None:
            return False, f"待验收目录不存在：{self.config.jav_staging_path}"
        if not match.isDirectory:
            return False, f"待验收路径不是目录：{self.config.jav_staging_path}"
        return True, ""

    async def submit_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_id = _positive_int(payload.get("batch_id"), "batch_id")
        callback_template = str(payload.get("callback_path") or "").strip()
        if callback_template != "/jav/magnet-queue/attempts/{attempt_id}":
            raise ValueError("callback_path 不受支持")
        items = payload.get("items")
        if not isinstance(items, list) or not items:
            raise ValueError("items 不能为空")
        if len(items) > 100:
            raise ValueError("单个下载批次最多 100 项")
        ready, reason = await self.check_ready()
        if not ready:
            raise RuntimeError(reason)

        tasks = []
        for raw in items:
            tasks.append(await self._submit_item(batch_id, callback_template, raw))
        return {"external_batch_id": f"cd2:{batch_id}", "tasks": tasks}

    async def _submit_item(
        self, batch_id: int, callback_template: str, raw: Any
    ) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValueError("下载项格式无效")
        attempt_id = _positive_int(raw.get("attempt_id"), "attempt_id")
        jav_id = _positive_int(raw.get("jav_id"), "jav_id")
        candidate_id = _positive_int(raw.get("candidate_id"), "candidate_id")
        code = str(raw.get("code") or "").strip()
        idempotency_key = str(raw.get("idempotency_key") or "").strip()
        magnet_uri = str(raw.get("magnet_uri") or "").strip()
        info_hash = magnet_info_hash(magnet_uri)
        if not code or not idempotency_key or not info_hash:
            raise ValueError("下载项缺少有效番号、幂等键或磁链 info hash")
        callback_path = callback_template.replace("{attempt_id}", str(attempt_id))
        proposed = DownloadJob(
            idempotency_key=idempotency_key,
            batch_id=batch_id,
            attempt_id=attempt_id,
            jav_id=jav_id,
            candidate_id=candidate_id,
            code=code,
            magnet_uri=magnet_uri,
            info_hash=info_hash,
            callback_path=callback_path,
            status=JOB_RECEIVED,
        )
        job, created = await self.repository.create_job(proposed)
        if not _same_job_identity(job, proposed):
            raise ValueError("attempt_id 或 idempotency_key 已绑定到不同下载项")
        if not created:
            return job.task_response()

        job = await self.repository.update_job(
            idempotency_key, status=JOB_SUBMITTING
        )
        try:
            paths = await self.cd2.add_offline(
                magnet_uri, self.config.jav_staging_path
            )
            job = await self.repository.update_job(
                idempotency_key,
                status=JOB_SUBMITTED,
                result_paths=paths,
            )
        except CloudDriveError as error:
            status = JOB_UNCERTAIN if error.uncertain else JOB_FAILED
            job = await self.repository.update_job(
                idempotency_key, status=status, error=str(error)
            )
            await self._notify(job)
        return job.task_response()

    async def poll_downloads(self) -> None:
        if self._poll_lock.locked():
            return
        async with self._poll_lock:
            jobs = await self.repository.list_jobs(
                (JOB_SUBMITTING, JOB_SUBMITTED, JOB_UNCERTAIN)
            )
            if not jobs:
                return
            try:
                offline_tasks = await self.cd2.list_offline(
                    self.config.jav_staging_path
                )
            except Exception as error:
                logger.warning("查询 JAV 离线任务失败：%s", error)
                return
            by_hash = {task.info_hash: task for task in offline_tasks if task.info_hash}
            for job in jobs:
                task = by_hash.get(job.info_hash)
                if task is None:
                    continue
                await self._apply_offline_task(job, task)

    async def _apply_offline_task(
        self, job: DownloadJob, task: OfflineTask
    ) -> None:
        if task.status == clouddrive_pb2.OFFLINE_ERROR:
            updated = await self.repository.update_job(
                job.idempotency_key,
                status=JOB_FAILED,
                error="CloudDrive2 离线任务失败",
            )
            await self._notify(updated)
            return
        if task.status == clouddrive_pb2.OFFLINE_FINISHED:
            paths = job.result_paths
            if not paths and task.name:
                paths = [posixpath.join(self.config.jav_staging_path, task.name)]
            updated = await self.repository.update_job(
                job.idempotency_key,
                status=JOB_AWAITING_QUALITY,
                result_paths=paths,
            )
            await self._notify(updated)
            return
        if job.status in (JOB_SUBMITTING, JOB_UNCERTAIN):
            updated = await self.repository.update_job(
                job.idempotency_key, status=JOB_SUBMITTED, error=""
            )
            await self._notify(updated)

    async def review_attempt(
        self, attempt_id: int, decision: str
    ) -> DownloadJob:
        job = await self.repository.get_job_by_attempt(attempt_id)
        if job is None:
            raise KeyError(attempt_id)
        decision = decision.strip().lower()
        if decision == "accepted":
            if job.status == JOB_AWAITING_SCAN:
                return job
            if job.status != JOB_AWAITING_QUALITY:
                raise ValueError(f"当前任务状态不能通过验收：{job.status}")
            paths = await self.cd2.move_from_staging(
                job.result_paths,
                self.config.jav_staging_path,
                self.config.jav_library_path,
            )
            return await self.repository.update_job(
                job.idempotency_key,
                status=JOB_AWAITING_SCAN,
                result_paths=paths,
                error="",
            )
        if decision == "rejected":
            if job.status == JOB_REJECTED:
                return job
            if job.status != JOB_AWAITING_QUALITY:
                raise ValueError(f"当前任务状态不能标记不合格：{job.status}")
            await self.cd2.delete_from_staging(
                job.result_paths, self.config.jav_staging_path
            )
            return await self.repository.update_job(
                job.idempotency_key,
                status=JOB_REJECTED,
                error="",
            )
        raise ValueError("decision 只能是 accepted 或 rejected")

    async def review_batch(self, items: list[dict[str, object]]) -> ReviewBatchResult:
        """Execute saved quality decisions with at most one move and one delete.

        The JavBoss side records the human decision before calling this method.
        A retry is idempotent: already promoted/rejected jobs are returned as
        they are and are not sent to CloudDrive2 again.
        """
        if not items:
            raise ValueError("items 不能为空")
        if len(items) > 100:
            raise ValueError("单个验收批次最多 100 项")

        jobs: list[tuple[DownloadJob, str]] = []
        seen_attempts: set[int] = set()
        for raw in items:
            if not isinstance(raw, dict):
                raise ValueError("验收项格式无效")
            try:
                attempt_id = int(raw.get("attempt_id") or 0)
            except (TypeError, ValueError) as error:
                raise ValueError("attempt_id 必须是正整数") from error
            if attempt_id <= 0 or attempt_id in seen_attempts:
                raise ValueError("attempt_id 必须是唯一正整数")
            decision = str(raw.get("decision") or "").strip().lower()
            if decision not in {"accepted", "rejected"}:
                raise ValueError("decision 只能是 accepted 或 rejected")
            job = await self.repository.get_job_by_attempt(attempt_id)
            if job is None:
                raise KeyError(attempt_id)
            if decision == "accepted" and job.status not in {
                JOB_AWAITING_QUALITY,
                JOB_AWAITING_SCAN,
            }:
                raise ValueError(f"当前任务状态不能通过验收：{job.status}")
            if decision == "rejected" and job.status not in {
                JOB_AWAITING_QUALITY,
                JOB_REJECTED,
            }:
                raise ValueError(f"当前任务状态不能标记不合格：{job.status}")
            if job.status == JOB_AWAITING_QUALITY and not job.result_paths:
                raise ValueError(f"任务 {attempt_id} 没有可处理的暂存文件路径")
            seen_attempts.add(attempt_id)
            jobs.append((job, decision))

        accepted_paths = [
            path
            for job, decision in jobs
            if decision == "accepted" and job.status == JOB_AWAITING_QUALITY
            for path in job.result_paths
        ]
        rejected_paths = [
            path
            for job, decision in jobs
            if decision == "rejected" and job.status == JOB_AWAITING_QUALITY
            for path in job.result_paths
        ]
        cleanup_payload: dict[str, int] = {
            "task_folders_scanned": 0,
            "directories_scanned": 0,
            "files_scanned": 0,
            "files_deleted": 0,
            "small_files_deleted": 0,
            "blacklist_files_deleted": 0,
            "files_kept": 0,
            "folders_deleted": 0,
            "errors": 0,
        }
        if accepted_paths:
            cleanup = await clean_jav_paths(
                self.cd2,
                self.config.jav_staging_path,
                accepted_paths,
                size_threshold_mb=_size_threshold_mb(),
                blacklist=load_blacklist(os.getenv("BLACKLIST_FILE", "blacklist.txt")),
            )
            cleanup_payload = cleanup.as_dict()
            if cleanup.errors:
                raise CloudDriveError(
                    "验收前清扫待验收目录失败："
                    f"{cleanup.errors} 个任务目录未完成清扫"
                )
            deleted = {str(path).strip() for path in cleanup.deleted_paths}
            missing = [
                path for path in accepted_paths if str(path).strip() in deleted
            ]
            if missing:
                raise CloudDriveError(
                    "清扫后通过作品没有可移动的文件："
                    + ", ".join(posixpath.basename(path) for path in missing)
                )
        moved_paths: list[str] = []
        if accepted_paths:
            moved_paths = await self.cd2.move_from_staging(
                accepted_paths,
                self.config.jav_staging_path,
                self.config.jav_library_path,
            )
        if rejected_paths:
            await self.cd2.delete_from_staging(
                rejected_paths, self.config.jav_staging_path
            )

        result: list[DownloadJob] = []
        moved_cursor = 0
        for job, decision in jobs:
            if decision == "accepted" and job.status == JOB_AWAITING_QUALITY:
                path_count = len(job.result_paths)
                promoted_paths = moved_paths[moved_cursor : moved_cursor + path_count]
                moved_cursor += path_count
                if len(promoted_paths) != path_count:
                    promoted_paths = [
                        posixpath.join(
                            self.config.jav_library_path, posixpath.basename(path)
                        )
                        for path in job.result_paths
                    ]
                job = await self.repository.update_job(
                    job.idempotency_key,
                    status=JOB_AWAITING_SCAN,
                    error="",
                    result_paths=promoted_paths,
                )
            elif decision == "rejected" and job.status == JOB_AWAITING_QUALITY:
                job = await self.repository.update_job(
                    job.idempotency_key,
                    status=JOB_REJECTED,
                    error="",
                )
            result.append(job)
        return ReviewBatchResult(jobs=result, cleanup=cleanup_payload)

    async def _notify(self, job: DownloadJob) -> None:
        if self.config.callback_enabled:
            payload = {
                "status": job.status,
                "external_task_id": job.info_hash,
                "error": job.error,
                "result_paths": job.result_paths,
            }
            try:
                await self.javboss.update_download_attempt(job.callback_path, payload)
            except Exception as error:
                logger.warning(
                    "JavBoss 回调暂存到 outbox attempt_id=%d status=%s: %s",
                    job.attempt_id,
                    job.status,
                    error,
                )
                await self.repository.enqueue_callback(
                    job.attempt_id, job.status, payload, str(error)
                )
        if self._status_notifier is not None:
            try:
                # Pass an immutable snapshot to user-facing integrations. A
                # repository refresh may mutate/reuse the same job instance
                # after this callback returns; notifications must describe
                # the transition that triggered them, not a later state.
                await self._status_notifier(copy.deepcopy(job))
            except Exception as error:  # pragma: no cover - Telegram/network dependent
                logger.warning(
                    "JAV 下载状态通知失败 attempt_id=%d status=%s: %s",
                    job.attempt_id,
                    job.status,
                    error,
                )

    async def flush_callbacks(self) -> None:
        if not self.config.callback_enabled:
            return
        rows = await self.repository.due_callbacks(utc_now_text())
        for row in rows:
            job = await self.repository.get_job_by_attempt(int(row["attempt_id"]))
            if job is None:
                await self.repository.delete_callback(int(row["id"]))
                continue
            try:
                await self.javboss.update_download_attempt(
                    job.callback_path, json.loads(row["payload"])
                )
            except Exception as error:
                attempts = int(row["attempts"]) + 1
                delay = min(
                    self.config.callback_retry_seconds * (2 ** min(attempts - 1, 6)),
                    3600,
                )
                next_at = datetime.now(timezone.utc) + timedelta(seconds=delay)
                await self.repository.postpone_callback(
                    int(row["id"]), next_at.isoformat(), str(error)
                )
                continue
            await self.repository.delete_callback(int(row["id"]))


def _positive_int(value: Any, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field} 必须是正整数") from error
    if parsed <= 0:
        raise ValueError(f"{field} 必须是正整数")
    return parsed


def _size_threshold_mb() -> int:
    raw = os.getenv("SIZE_THRESHOLD", "300").strip()
    try:
        value = int(raw)
    except ValueError:
        return 300
    return max(value, 0)


def _same_job_identity(left: DownloadJob, right: DownloadJob) -> bool:
    return (
        left.idempotency_key == right.idempotency_key
        and left.attempt_id == right.attempt_id
        and left.jav_id == right.jav_id
        and left.candidate_id == right.candidate_id
        and left.info_hash == right.info_hash
    )
