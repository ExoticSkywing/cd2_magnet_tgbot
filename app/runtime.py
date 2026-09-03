from __future__ import annotations

import logging

from .cd2_client import CloudDriveClient
from .config import IntegrationConfig
from .download_service import DownloadGatewayService
from .http_api import GatewayHTTPServer
from .javboss_client import JavBossClient
from .repository import IntegrationRepository

logger = logging.getLogger(__name__)


class IntegrationRuntime:
    def __init__(self, config: IntegrationConfig):
        self.config = config
        self.repository = IntegrationRepository(config.state_db_path)
        self.cd2 = CloudDriveClient(config.cd2_address, config.cd2_token)
        self.javboss = JavBossClient(
            config.javboss_base_url,
            input_token=config.javboss_input_token,
            callback_token=config.javboss_callback_token,
        )
        self.downloads = DownloadGatewayService(
            config, self.repository, self.cd2, self.javboss
        )
        self.http = GatewayHTTPServer(config, self.downloads)

    async def start(self) -> None:
        await self.repository.open()
        await self.cd2.open()
        await self.javboss.open()
        await self.http.start()
        ready, reason = await self.downloads.check_ready()
        if ready:
            logger.info(
                "JAV 下载网关已就绪，监听 %s:%d，待验收目录=%s",
                self.config.gateway_host,
                self.config.gateway_port,
                self.config.jav_staging_path,
            )
        else:
            logger.warning("JAV 下载网关尚未就绪：%s", reason)

    async def close(self) -> None:
        await self.http.close()
        await self.javboss.close()
        await self.cd2.close()
        await self.repository.close()

    async def poll_downloads_job(self, _context=None) -> None:
        await self.downloads.poll_downloads()

    async def flush_callbacks_job(self, _context=None) -> None:
        await self.downloads.flush_callbacks()
