from __future__ import annotations

import hmac
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aiohttp import web

from .cd2_client import CloudDriveError
from .config import IntegrationConfig
from .download_service import DownloadGatewayService, ReviewBatchResult


logger = logging.getLogger(__name__)


class GatewayHTTPServer:
    def __init__(
        self, config: IntegrationConfig, service: DownloadGatewayService
    ) -> None:
        self.config = config
        self.service = service
        self._runner: web.AppRunner | None = None
        self._review_notifier: Callable[[ReviewBatchResult], Awaitable[None]] | None = None

    def set_review_notifier(
        self, notifier: Callable[[ReviewBatchResult], Awaitable[None]] | None
    ) -> None:
        """设置验收完成后的异步通知回调。通知失败不影响验收结果。"""

        self._review_notifier = notifier

    async def start(self) -> None:
        if self._runner is not None:
            return
        app = web.Application(client_max_size=2 * 1024 * 1024)
        app.router.add_get("/healthz", self.health)
        app.router.add_get("/readyz", self.ready)
        app.router.add_post(
            "/v1/javboss/download-batches", self.submit_download_batch
        )
        app.router.add_post(
            "/v1/javboss/download-attempts/{attempt_id}/review",
            self.review_download_attempt,
        )
        app.router.add_post(
            "/v1/javboss/download-attempts/review-batch",
            self.review_download_batch,
        )
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(
            self._runner, self.config.gateway_host, self.config.gateway_port
        )
        await site.start()

    async def close(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
        self._runner = None

    async def health(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def ready(self, _request: web.Request) -> web.Response:
        ready, reason = await self.service.check_ready()
        status = 200 if ready else 503
        return web.json_response(
            {
                "status": "ready" if ready else "not_ready",
                "reason": reason,
                "staging_path": self.config.jav_staging_path,
                "library_path": self.config.jav_library_path,
            },
            status=status,
        )

    async def submit_download_batch(self, request: web.Request) -> web.Response:
        unauthorized = self._authorize(request)
        if unauthorized is not None:
            return unauthorized
        try:
            payload = await request.json()
            if not isinstance(payload, dict):
                raise ValueError("请求体必须是 JSON 对象")
            response = await self.service.submit_batch(payload)
            return web.json_response(response, status=202)
        except ValueError as error:
            return _error_response(400, str(error))
        except CloudDriveError as error:
            return _error_response(502, str(error))
        except RuntimeError as error:
            return _error_response(503, str(error))

    async def review_download_attempt(self, request: web.Request) -> web.Response:
        unauthorized = self._authorize(request)
        if unauthorized is not None:
            return unauthorized
        try:
            attempt_id = int(request.match_info["attempt_id"])
            if attempt_id <= 0:
                raise ValueError("attempt_id 必须是正整数")
            payload: Any = await request.json()
            if not isinstance(payload, dict):
                raise ValueError("请求体必须是 JSON 对象")
            job = await self.service.review_attempt(
                attempt_id, str(payload.get("decision") or "")
            )
            return web.json_response({"task": job.task_response()})
        except KeyError:
            return _error_response(404, "下载任务不存在")
        except ValueError as error:
            return _error_response(409, str(error))
        except CloudDriveError as error:
            return _error_response(502, str(error))

    async def review_download_batch(self, request: web.Request) -> web.Response:
        unauthorized = self._authorize(request)
        if unauthorized is not None:
            return unauthorized
        try:
            payload: Any = await request.json()
            if not isinstance(payload, dict):
                raise ValueError("请求体必须是 JSON 对象")
            items = payload.get("items")
            if not isinstance(items, list):
                raise ValueError("items 必须是数组")
            logger.info(
                "JavBoss 验收批次开始：任务数=%d attempt_ids=%s",
                len(items),
                [item.get("attempt_id") for item in items if isinstance(item, dict)],
            )
            result = await self.service.review_batch(items)
            if self._review_notifier is not None:
                try:
                    await self._review_notifier(result)
                except Exception as error:  # pragma: no cover - Telegram/network dependent
                    # The CD2 operation and JavBoss state are already durable;
                    # a Telegram outage must never turn a successful review
                    # into a retry that repeats storage operations.
                    logger.warning("JavBoss 验收通知发送失败：%s", error)
            logger.info(
                "JavBoss 验收批次完成：任务数=%d cleanup=%s",
                len(result.jobs), result.cleanup,
            )
            return web.json_response(
                {
                    "items": [job.task_response() for job in result.jobs],
                    "cleanup": result.cleanup,
                }
            )
        except KeyError:
            logger.warning("JavBoss 验收批次失败：任务不存在")
            return _error_response(404, "下载任务不存在")
        except ValueError as error:
            logger.warning("JavBoss 验收批次参数/状态失败：%s", error)
            return _error_response(409, str(error))
        except CloudDriveError as error:
            logger.error("JavBoss 验收批次 CloudDrive2 操作失败：%s", error)
            return _error_response(502, str(error))

    def _authorize(self, request: web.Request) -> web.Response | None:
        expected = self.config.gateway_token
        header = request.headers.get("Authorization", "")
        scheme, separator, provided = header.partition(" ")
        if (
            not expected
            or not separator
            or scheme.lower() != "bearer"
            or not hmac.compare_digest(provided.strip(), expected)
        ):
            return _error_response(401, "下载网关密钥无效")
        return None


def _error_response(status: int, message: str) -> web.Response:
    return web.json_response({"error": message}, status=status)
