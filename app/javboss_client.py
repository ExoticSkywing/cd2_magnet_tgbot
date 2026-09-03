from __future__ import annotations

from typing import Any

import aiohttp


class JavBossRequestError(RuntimeError):
    def __init__(self, message: str, *, status: int = 0, payload: Any = None):
        super().__init__(message)
        self.status = status
        self.payload = payload


class JavBossClient:
    def __init__(
        self,
        base_url: str,
        *,
        input_token: str = "",
        callback_token: str = "",
        timeout_seconds: float = 20.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.input_token = input_token
        self.callback_token = callback_token
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: aiohttp.ClientSession | None = None

    async def open(self) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self.timeout)

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
        self._session = None

    def _client(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("JavBoss client is not open")
        return self._session

    async def submit_jav_input(
        self, raw_input: str, *, idempotency_key: str
    ) -> dict[str, Any]:
        if not self.input_token:
            raise JavBossRequestError("尚未配置 JavBoss 番号输入密钥")
        return await self._json_request(
            "POST",
            "/integrations/telegram/jav/input-batches",
            token=self.input_token,
            headers={"Idempotency-Key": idempotency_key},
            payload={"raw_input": raw_input},
        )

    async def update_download_attempt(
        self, callback_path: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        if not self.callback_token:
            raise JavBossRequestError("尚未配置 JavBoss 下载回调密钥")
        if not callback_path.startswith("/jav/magnet-queue/attempts/"):
            raise JavBossRequestError("JavBoss 回调路径不受支持")
        return await self._json_request(
            "PATCH", callback_path, token=self.callback_token, payload=payload
        )

    async def _json_request(
        self,
        method: str,
        path: str,
        *,
        token: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        request_headers = {"Authorization": f"Bearer {token}"}
        request_headers.update(headers or {})
        try:
            async with self._client().request(
                method,
                self.base_url + path,
                json=payload,
                headers=request_headers,
            ) as response:
                try:
                    body = await response.json(content_type=None)
                except (aiohttp.ContentTypeError, ValueError):
                    body = {}
                if response.status < 200 or response.status >= 300:
                    message = (
                        body.get("error_zh")
                        or body.get("error")
                        or f"JavBoss 返回 HTTP {response.status}"
                    )
                    raise JavBossRequestError(
                        str(message), status=response.status, payload=body
                    )
                return body if isinstance(body, dict) else {}
        except JavBossRequestError:
            raise
        except (aiohttp.ClientError, TimeoutError) as error:
            raise JavBossRequestError(f"无法连接 JavBoss：{error}") from error
