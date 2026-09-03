from __future__ import annotations

import os
import posixpath
from dataclasses import dataclass, field
from typing import Iterable

from .cd2_client import CloudDriveClient, ensure_path_below, normalize_cloud_path


@dataclass
class JavCleanupStats:
    """统计一次验收前清扫，供日志和网关响应使用。"""

    task_folders_scanned: int = 0
    directories_scanned: int = 0
    files_scanned: int = 0
    files_deleted: int = 0
    small_files_deleted: int = 0
    blacklist_files_deleted: int = 0
    files_kept: int = 0
    folders_deleted: int = 0
    errors: int = 0
    deleted_paths: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, int]:
        return {
            "task_folders_scanned": self.task_folders_scanned,
            "directories_scanned": self.directories_scanned,
            "files_scanned": self.files_scanned,
            "files_deleted": self.files_deleted,
            "small_files_deleted": self.small_files_deleted,
            "blacklist_files_deleted": self.blacklist_files_deleted,
            "files_kept": self.files_kept,
            "folders_deleted": self.folders_deleted,
            "errors": self.errors,
        }


def load_blacklist(path: str = "blacklist.txt") -> list[str]:
    """读取与 Telegram /clean 相同的黑名单文件。"""

    blacklist_path = os.path.abspath(path)
    if not os.path.exists(blacklist_path):
        return ["广告", "promo", ".url", "txt", "readme", "扫码", "最新地址"]
    with open(blacklist_path, "r", encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]


def _task_roots(paths: Iterable[str], staging_root: str) -> list[str]:
    """把文件/目录路径收敛为待处理的 JavBoss 任务目录。"""

    root = normalize_cloud_path(staging_root)
    result: list[str] = []
    seen: set[str] = set()
    for raw_path in paths:
        normalized = ensure_path_below(str(raw_path), root)
        relative = normalized[len(root) + 1 :]
        task_name = relative.split("/", 1)[0].strip()
        if not task_name:
            continue
        task_path = posixpath.join(root, task_name)
        if task_path not in seen:
            seen.add(task_path)
            result.append(task_path)
    return result


async def _walk_files(
    cd2: CloudDriveClient, folder_path: str
) -> tuple[list[object], list[object]]:
    files: list[object] = []
    directories: list[object] = []
    pending = [folder_path]
    visited: set[str] = set()
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        rows = await cd2.list_children(current, force_refresh=True)
        for row in rows:
            child_path = str(getattr(row, "fullPathName", "") or "").strip()
            if not child_path:
                continue
            # The API should return descendants, but reject malformed or stale
            # paths so a cleanup request can never escape the selected task.
            try:
                ensure_path_below(child_path, folder_path)
            except ValueError:
                continue
            if bool(getattr(row, "isDirectory", False)):
                directories.append(row)
                pending.append(child_path)
            else:
                files.append(row)
    return files, directories


async def _is_empty(cd2: CloudDriveClient, path: str) -> bool:
    rows = await cd2.list_children(path, force_refresh=True)
    return len(rows) == 0


async def clean_jav_paths(
    cd2: CloudDriveClient,
    staging_root: str,
    paths: Iterable[str],
    *,
    size_threshold_mb: int = 300,
    blacklist: Iterable[str] = (),
) -> JavCleanupStats:
    """清扫即将通过验收的任务目录。

    只处理 ``paths`` 所属的 JavBoss 任务目录，不扫描待验收根目录下的
    其它任务，也不删除任务根目录本身。这样批量验收时不会误触碰尚未
    判断的作品；真正移动/删除仍由验收批次负责。
    """

    root = normalize_cloud_path(staging_root)
    roots = _task_roots(paths, root)
    stats = JavCleanupStats()
    threshold_bytes = max(int(size_threshold_mb), 0) * 1024 * 1024
    blacklist_values = [str(value).strip().lower() for value in blacklist if str(value).strip()]

    for folder_path in roots:
        stats.task_folders_scanned += 1
        try:
            files, directories = await _walk_files(cd2, folder_path)
            stats.files_scanned += len(files)
            stats.directories_scanned += len(directories) + 1
            files_to_delete: list[str] = []
            small_count = 0
            blacklist_count = 0
            for row in files:
                file_path = str(getattr(row, "fullPathName", "") or "").strip()
                file_name = str(getattr(row, "name", "") or "")
                size = int(getattr(row, "size", 0) or 0)
                if size < threshold_bytes:
                    files_to_delete.append(file_path)
                    small_count += 1
                elif any(word in file_name.lower() for word in blacklist_values):
                    files_to_delete.append(file_path)
                    blacklist_count += 1

            stats.files_kept += len(files) - len(files_to_delete)
            if files_to_delete:
                await cd2.delete_from_staging(files_to_delete, root)
                stats.files_deleted += len(files_to_delete)
                stats.small_files_deleted += small_count
                stats.blacklist_files_deleted += blacklist_count
                stats.deleted_paths.extend(files_to_delete)

            # Empty child directories are safe to remove. The task root is
            # intentionally retained so a stale/missing download cannot be
            # mistaken for a successful promotion.
            directories.sort(
                key=lambda row: str(getattr(row, "fullPathName", "")).count("/"),
                reverse=True,
            )
            empty_dirs: list[str] = []
            for row in directories:
                directory_path = str(getattr(row, "fullPathName", "") or "").strip()
                if directory_path and await _is_empty(cd2, directory_path):
                    empty_dirs.append(directory_path)
            if empty_dirs:
                await cd2.delete_from_staging(empty_dirs, root)
                stats.folders_deleted += len(empty_dirs)
                stats.deleted_paths.extend(empty_dirs)
        except Exception:
            stats.errors += 1

    return stats
