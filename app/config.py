from __future__ import annotations

import os
from dataclasses import dataclass


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} 必须是大于 0 的整数")
    return value


def _cloud_path(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    if not value.startswith("/") or value == "/":
        raise ValueError(f"{name} 必须是 CloudDrive2 内的绝对目录路径")
    return "/" + "/".join(part for part in value.split("/") if part)


@dataclass(frozen=True)
class IntegrationConfig:
    cd2_address: str
    cd2_token: str
    jav_staging_path: str
    jav_library_path: str
    gateway_host: str
    gateway_port: int
    gateway_token: str
    state_db_path: str
    javboss_base_url: str
    javboss_input_token: str
    javboss_callback_token: str
    poll_interval_seconds: int
    callback_retry_seconds: int

    @classmethod
    def from_env(cls) -> "IntegrationConfig":
        return cls(
            cd2_address=os.getenv("CD2_ADDRESS", "127.0.0.1:19798").strip(),
            cd2_token=os.getenv("CD2_TOKEN", "").strip(),
            jav_staging_path=_cloud_path(
                "JAV_STAGING_PATH", "/115/云下载/jav待验收"
            ),
            jav_library_path=_cloud_path(
                "JAV_LIBRARY_PATH", "/115/upload/javbosstest"
            ),
            gateway_host=os.getenv("GATEWAY_HOST", "127.0.0.1").strip()
            or "127.0.0.1",
            gateway_port=_positive_int("GATEWAY_PORT", 18081),
            gateway_token=os.getenv("JAVBOSS_GATEWAY_TOKEN", "").strip(),
            state_db_path=os.getenv(
                "INTEGRATION_STATE_DB", "/app/data/integration.db"
            ).strip(),
            javboss_base_url=os.getenv(
                "JAVBOSS_BASE_URL", "http://127.0.0.1:17654"
            ).strip().rstrip("/"),
            javboss_input_token=os.getenv("JAVBOSS_INPUT_TOKEN", "").strip(),
            javboss_callback_token=os.getenv(
                "JAVBOSS_CALLBACK_TOKEN", ""
            ).strip(),
            poll_interval_seconds=_positive_int("JAV_DOWNLOAD_POLL_SECONDS", 30),
            callback_retry_seconds=_positive_int("JAV_CALLBACK_RETRY_SECONDS", 30),
        )

    @property
    def gateway_enabled(self) -> bool:
        return bool(self.cd2_token and self.gateway_token)

    @property
    def jav_input_enabled(self) -> bool:
        return bool(self.javboss_base_url and self.javboss_input_token)

    @property
    def callback_enabled(self) -> bool:
        return bool(self.javboss_base_url and self.javboss_callback_token)
