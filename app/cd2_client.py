from __future__ import annotations

import posixpath
from dataclasses import dataclass
from typing import Iterable

import grpc

import clouddrive_pb2
import clouddrive_pb2_grpc


class CloudDriveError(RuntimeError):
    def __init__(self, message: str, *, uncertain: bool = False):
        super().__init__(message)
        self.uncertain = uncertain


@dataclass(frozen=True)
class OfflineTask:
    name: str
    url: str
    info_hash: str
    status: int
    percent_done: float


def normalize_cloud_path(value: str) -> str:
    normalized = posixpath.normpath("/" + str(value or "").lstrip("/"))
    if normalized == "/" or normalized.startswith("/../"):
        raise ValueError("CloudDrive path must point below root")
    return normalized


def ensure_path_below(path: str, root: str) -> str:
    normalized_path = normalize_cloud_path(path)
    normalized_root = normalize_cloud_path(root)
    if normalized_path == normalized_root or not normalized_path.startswith(
        normalized_root + "/"
    ):
        raise ValueError(f"path is outside the managed staging directory: {path}")
    return normalized_path


class CloudDriveClient:
    def __init__(self, address: str, token: str, *, timeout: float = 20.0):
        self.address = address
        self.token = token
        self.timeout = timeout
        self._channel: grpc.aio.Channel | None = None
        self._stub: clouddrive_pb2_grpc.CloudDriveFileSrvStub | None = None

    async def open(self) -> None:
        if self._channel is not None:
            return
        self._channel = grpc.aio.insecure_channel(self.address)
        self._stub = clouddrive_pb2_grpc.CloudDriveFileSrvStub(self._channel)

    async def close(self) -> None:
        if self._channel is not None:
            await self._channel.close()
        self._channel = None
        self._stub = None

    def _service(self):
        if self._stub is None:
            raise RuntimeError("CloudDrive client is not open")
        return self._stub

    def _metadata(self) -> tuple[tuple[str, str], ...]:
        return (("authorization", f"Bearer {self.token}"),)

    async def list_children(self, path: str, *, force_refresh: bool = False) -> list:
        request = clouddrive_pb2.ListSubFileRequest(
            path=normalize_cloud_path(path), forceRefresh=force_refresh
        )
        rows = []
        try:
            stream = self._service().GetSubFiles(
                request, metadata=self._metadata(), timeout=self.timeout
            )
            async for reply in stream:
                rows.extend(reply.subFiles)
            return rows
        except grpc.aio.AioRpcError as error:
            raise self._rpc_error("list directory", error) from error

    async def ensure_directory(self, path: str) -> None:
        normalized = normalize_cloud_path(path)
        parent, name = posixpath.split(normalized)
        rows = await self.list_children(parent, force_refresh=True)
        for row in rows:
            if row.name != name:
                continue
            if not row.isDirectory:
                raise CloudDriveError(f"目标路径已存在但不是目录: {normalized}")
            return
        request = clouddrive_pb2.CreateFolderRequest(
            parentPath=parent, folderName=name
        )
        try:
            result = await self._service().CreateFolder(
                request, metadata=self._metadata(), timeout=self.timeout
            )
        except grpc.aio.AioRpcError as error:
            raise self._rpc_error("create directory", error) from error
        if not result.result.success:
            raise CloudDriveError(
                result.result.errorMessage or f"无法创建目录: {normalized}"
            )

    async def add_offline(self, uri: str, destination: str) -> list[str]:
        request = clouddrive_pb2.AddOfflineFileRequest(
            urls=uri,
            toFolder=normalize_cloud_path(destination),
            checkFolderAfterSecs=5,
        )
        try:
            result = await self._service().AddOfflineFiles(
                request, metadata=self._metadata(), timeout=self.timeout
            )
        except grpc.aio.AioRpcError as error:
            raise self._rpc_error("submit offline download", error) from error
        if not result.success:
            raise CloudDriveError(result.errorMessage or "CloudDrive2 拒绝离线下载")
        return [str(path).strip() for path in result.resultFilePaths if str(path).strip()]

    async def list_offline(self, path: str) -> list[OfflineTask]:
        request = clouddrive_pb2.FileRequest(path=normalize_cloud_path(path))
        try:
            result = await self._service().ListOfflineFilesByPath(
                request, metadata=self._metadata(), timeout=self.timeout
            )
        except grpc.aio.AioRpcError as error:
            raise self._rpc_error("list offline downloads", error) from error
        return [
            OfflineTask(
                name=row.name,
                url=row.url,
                info_hash=row.infoHash.lower().strip(),
                status=row.status,
                percent_done=row.percendDone,
            )
            for row in result.offlineFiles
        ]

    async def move_from_staging(
        self, paths: Iterable[str], staging_root: str, destination: str
    ) -> list[str]:
        normalized = [ensure_path_below(path, staging_root) for path in paths]
        if not normalized:
            raise CloudDriveError("没有可晋升的暂存文件")
        request = clouddrive_pb2.MoveFileRequest(
            theFilePaths=normalized,
            destPath=normalize_cloud_path(destination),
            conflictPolicy=clouddrive_pb2.MoveFileRequest.Skip,
            handleConflictRecursively=True,
        )
        try:
            result = await self._service().MoveFile(
                request, metadata=self._metadata(), timeout=self.timeout
            )
        except grpc.aio.AioRpcError as error:
            raise self._rpc_error("promote staged files", error) from error
        if not result.success:
            raise CloudDriveError(result.errorMessage or "移动暂存文件失败")
        returned = [
            str(path).strip() for path in result.resultFilePaths if str(path).strip()
        ]
        if returned:
            return returned
        destination = normalize_cloud_path(destination)
        return [posixpath.join(destination, posixpath.basename(path)) for path in normalized]

    async def delete_from_staging(
        self, paths: Iterable[str], staging_root: str
    ) -> None:
        normalized = [ensure_path_below(path, staging_root) for path in paths]
        if not normalized:
            raise CloudDriveError("没有可删除的暂存文件")
        request = clouddrive_pb2.MultiFileRequest(path=normalized)
        try:
            result = await self._service().DeleteFiles(
                request, metadata=self._metadata(), timeout=self.timeout
            )
        except grpc.aio.AioRpcError as error:
            raise self._rpc_error("delete staged files", error) from error
        if not result.success:
            raise CloudDriveError(result.errorMessage or "删除暂存文件失败")

    @staticmethod
    def _rpc_error(operation: str, error: grpc.aio.AioRpcError) -> CloudDriveError:
        uncertain_codes = {
            grpc.StatusCode.UNAVAILABLE,
            grpc.StatusCode.DEADLINE_EXCEEDED,
            grpc.StatusCode.CANCELLED,
            grpc.StatusCode.INTERNAL,
        }
        details = (error.details() or error.code().name).strip()
        return CloudDriveError(
            f"CloudDrive2 {operation} failed: {details}",
            uncertain=error.code() in uncertain_codes,
        )
