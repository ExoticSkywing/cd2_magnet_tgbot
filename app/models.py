from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


JOB_RECEIVED = "received"
JOB_SUBMITTING = "submitting"
JOB_SUBMITTED = "submitted"
JOB_AWAITING_QUALITY = "awaiting_quality"
JOB_AWAITING_SCAN = "awaiting_scan"
JOB_REJECTED = "rejected"
JOB_FAILED = "failed"
JOB_UNCERTAIN = "uncertain"


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class DownloadJob:
    idempotency_key: str
    batch_id: int
    attempt_id: int
    jav_id: int
    candidate_id: int
    code: str
    magnet_uri: str
    info_hash: str
    callback_path: str
    status: str = JOB_RECEIVED
    result_paths: list[str] = field(default_factory=list)
    error: str = ""
    created_at: str = field(default_factory=utc_now_text)
    updated_at: str = field(default_factory=utc_now_text)

    @classmethod
    def from_row(cls, row: Any) -> "DownloadJob":
        return cls(
            idempotency_key=row["idempotency_key"],
            batch_id=row["batch_id"],
            attempt_id=row["attempt_id"],
            jav_id=row["jav_id"],
            candidate_id=row["candidate_id"],
            code=row["code"],
            magnet_uri=row["magnet_uri"],
            info_hash=row["info_hash"],
            callback_path=row["callback_path"],
            status=row["status"],
            result_paths=json.loads(row["result_paths"] or "[]"),
            error=row["error"] or "",
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def task_response(self) -> dict[str, Any]:
        return {
            "attempt_id": self.attempt_id,
            "idempotency_key": self.idempotency_key,
            "external_task_id": self.info_hash,
            "status": self.status,
            "error": self.error,
            "result_paths": self.result_paths,
        }
