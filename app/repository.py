from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Iterable

import aiosqlite

from .models import DownloadJob, utc_now_text


class IntegrationRepository:
    def __init__(self, path: str):
        self.path = path
        self._db: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        parent = os.path.dirname(self.path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS download_job (
                idempotency_key TEXT PRIMARY KEY,
                batch_id INTEGER NOT NULL,
                attempt_id INTEGER NOT NULL UNIQUE,
                jav_id INTEGER NOT NULL,
                candidate_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                magnet_uri TEXT NOT NULL,
                info_hash TEXT NOT NULL,
                callback_path TEXT NOT NULL,
                status TEXT NOT NULL,
                result_paths TEXT NOT NULL DEFAULT '[]',
                error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_download_job_status
                ON download_job(status, updated_at);
            CREATE TABLE IF NOT EXISTS callback_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                attempt_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL,
                last_error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                UNIQUE(attempt_id, status)
            );
            """
        )
        await self._db.commit()

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    def _connection(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("integration repository is not open")
        return self._db

    async def create_job(self, job: DownloadJob) -> tuple[DownloadJob, bool]:
        async with self._lock:
            db = self._connection()
            cursor = await db.execute(
                """
                INSERT OR IGNORE INTO download_job (
                    idempotency_key, batch_id, attempt_id, jav_id, candidate_id,
                    code, magnet_uri, info_hash, callback_path, status,
                    result_paths, error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.idempotency_key,
                    job.batch_id,
                    job.attempt_id,
                    job.jav_id,
                    job.candidate_id,
                    job.code,
                    job.magnet_uri,
                    job.info_hash,
                    job.callback_path,
                    job.status,
                    json.dumps(job.result_paths, ensure_ascii=False),
                    job.error,
                    job.created_at,
                    job.updated_at,
                ),
            )
            created = cursor.rowcount > 0
            await cursor.close()
            await db.commit()
            stored = await self._get_job_locked(job.idempotency_key)
            if stored is None:
                # attempt_id is also unique. Surface an identity mismatch rather
                # than creating a second CloudDrive task for an existing attempt.
                stored = await self._get_job_by_attempt_locked(job.attempt_id)
            if stored is None:
                raise RuntimeError("failed to load persisted download job")
            return stored, created

    async def get_job(self, idempotency_key: str) -> DownloadJob | None:
        async with self._lock:
            return await self._get_job_locked(idempotency_key)

    async def get_job_by_attempt(self, attempt_id: int) -> DownloadJob | None:
        async with self._lock:
            return await self._get_job_by_attempt_locked(attempt_id)

    async def _get_job_locked(self, idempotency_key: str) -> DownloadJob | None:
        cursor = await self._connection().execute(
            "SELECT * FROM download_job WHERE idempotency_key = ?",
            (idempotency_key,),
        )
        row = await cursor.fetchone()
        await cursor.close()
        return DownloadJob.from_row(row) if row else None

    async def _get_job_by_attempt_locked(self, attempt_id: int) -> DownloadJob | None:
        cursor = await self._connection().execute(
            "SELECT * FROM download_job WHERE attempt_id = ?", (attempt_id,)
        )
        row = await cursor.fetchone()
        await cursor.close()
        return DownloadJob.from_row(row) if row else None

    async def update_job(
        self,
        idempotency_key: str,
        *,
        status: str,
        result_paths: list[str] | None = None,
        error: str = "",
    ) -> DownloadJob:
        async with self._lock:
            db = self._connection()
            fields = ["status = ?", "error = ?", "updated_at = ?"]
            values: list[Any] = [status, error, utc_now_text()]
            if result_paths is not None:
                fields.append("result_paths = ?")
                values.append(json.dumps(result_paths, ensure_ascii=False))
            values.append(idempotency_key)
            await db.execute(
                f"UPDATE download_job SET {', '.join(fields)} WHERE idempotency_key = ?",
                values,
            )
            await db.commit()
            job = await self._get_job_locked(idempotency_key)
            if job is None:
                raise KeyError(idempotency_key)
            return job

    async def list_jobs(self, statuses: Iterable[str]) -> list[DownloadJob]:
        values = tuple(dict.fromkeys(statuses))
        if not values:
            return []
        placeholders = ",".join("?" for _ in values)
        async with self._lock:
            cursor = await self._connection().execute(
                f"SELECT * FROM download_job WHERE status IN ({placeholders}) ORDER BY updated_at",
                values,
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [DownloadJob.from_row(row) for row in rows]

    async def enqueue_callback(self, attempt_id: int, status: str, payload: dict[str, Any], error: str) -> None:
        now = utc_now_text()
        async with self._lock:
            db = self._connection()
            await db.execute(
                """
                INSERT INTO callback_outbox (
                    attempt_id, status, payload, attempts, next_attempt_at,
                    last_error, created_at
                ) VALUES (?, ?, ?, 0, ?, ?, ?)
                ON CONFLICT(attempt_id, status) DO UPDATE SET
                    payload = excluded.payload,
                    next_attempt_at = excluded.next_attempt_at,
                    last_error = excluded.last_error
                """,
                (
                    attempt_id,
                    status,
                    json.dumps(payload, ensure_ascii=False),
                    now,
                    error,
                    now,
                ),
            )
            await db.commit()

    async def due_callbacks(self, now: str) -> list[dict[str, Any]]:
        async with self._lock:
            cursor = await self._connection().execute(
                "SELECT * FROM callback_outbox WHERE next_attempt_at <= ? ORDER BY id LIMIT 100",
                (now,),
            )
            rows = await cursor.fetchall()
            await cursor.close()
            return [dict(row) for row in rows]

    async def delete_callback(self, callback_id: int) -> None:
        async with self._lock:
            db = self._connection()
            await db.execute("DELETE FROM callback_outbox WHERE id = ?", (callback_id,))
            await db.commit()

    async def postpone_callback(self, callback_id: int, next_attempt_at: str, error: str) -> None:
        async with self._lock:
            db = self._connection()
            await db.execute(
                """
                UPDATE callback_outbox
                SET attempts = attempts + 1, next_attempt_at = ?, last_error = ?
                WHERE id = ?
                """,
                (next_attempt_at, error, callback_id),
            )
            await db.commit()
