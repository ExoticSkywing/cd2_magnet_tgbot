import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import clouddrive_pb2

from app.cd2_client import CloudDriveError, OfflineTask, ensure_path_below
from app.config import IntegrationConfig
from app.download_service import DownloadGatewayService
from app.jav_cleanup import clean_jav_paths
from app.models import (
    JOB_AWAITING_QUALITY,
    JOB_AWAITING_SCAN,
    JOB_FAILED,
    JOB_REJECTED,
    JOB_UNCERTAIN,
    DownloadJob,
)
from app.repository import IntegrationRepository


MAGNET = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"


def integration_config(db_path: str) -> IntegrationConfig:
    return IntegrationConfig(
        cd2_address="127.0.0.1:19798",
        cd2_token="cd2-token",
        jav_staging_path="/115/云下载/jav待验收",
        jav_library_path="/115/正式作品库",
        gateway_host="127.0.0.1",
        gateway_port=18080,
        gateway_token="gateway-token",
        state_db_path=db_path,
        javboss_base_url="http://127.0.0.1:17654",
        javboss_input_token="input-token",
        javboss_callback_token="callback-token",
        poll_interval_seconds=30,
        callback_retry_seconds=30,
    )


def batch_payload(*, key: str = "jav:11:candidate:21", attempt_id: int = 31):
    return {
        "batch_id": 41,
        "callback_path": "/jav/magnet-queue/attempts/{attempt_id}",
        "items": [
            {
                "attempt_id": attempt_id,
                "jav_id": 11,
                "candidate_id": 21,
                "code": "TEST-001",
                "magnet_uri": MAGNET,
                "idempotency_key": key,
            }
        ],
    }


class DownloadGatewayServiceTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "integration.db")
        self.config = integration_config(db_path)
        self.repository = IntegrationRepository(db_path)
        await self.repository.open()
        self.cd2 = SimpleNamespace(
            list_children=AsyncMock(
                return_value=[SimpleNamespace(name="jav待验收", isDirectory=True)]
            ),
            add_offline=AsyncMock(
                return_value=["/115/云下载/jav待验收/TEST-001"]
            ),
            move_from_staging=AsyncMock(
                return_value=["/115/正式作品库/TEST-001"]
            ),
            delete_from_staging=AsyncMock(return_value=None),
        )
        self.javboss = SimpleNamespace(update_download_attempt=AsyncMock())
        self.service = DownloadGatewayService(
            self.config, self.repository, self.cd2, self.javboss
        )

    async def asyncTearDown(self):
        await self.repository.close()
        self.temp_dir.cleanup()

    async def test_duplicate_idempotency_key_submits_to_cd2_once(self):
        first = await self.service.submit_batch(batch_payload())
        second = await self.service.submit_batch(batch_payload())

        self.assertEqual(first, second)
        self.cd2.add_offline.assert_awaited_once()

    async def test_attempt_identity_conflict_is_rejected(self):
        await self.service.submit_batch(batch_payload())

        with self.assertRaisesRegex(ValueError, "绑定到不同下载项"):
            await self.service.submit_batch(
                batch_payload(key="jav:other:candidate", attempt_id=31)
            )
        self.cd2.add_offline.assert_awaited_once()

    async def test_missing_staging_directory_is_not_ready(self):
        self.cd2.list_children.return_value = []

        ready, reason = await self.service.check_ready()

        self.assertFalse(ready)
        self.assertIn("待验收目录不存在", reason)

    async def test_uncertain_submit_is_persisted_without_blind_retry(self):
        self.cd2.add_offline.side_effect = CloudDriveError(
            "deadline exceeded", uncertain=True
        )

        result = await self.service.submit_batch(batch_payload())

        self.assertEqual(result["tasks"][0]["status"], JOB_UNCERTAIN)
        await self.service.submit_batch(batch_payload())
        self.cd2.add_offline.assert_awaited_once()

    async def test_callback_failure_is_written_to_outbox(self):
        self.javboss.update_download_attempt.side_effect = RuntimeError("offline")
        job = await self.repository.get_job(
            (await self.service.submit_batch(batch_payload()))["tasks"][0][
                "idempotency_key"
            ]
        )
        job = await self.repository.update_job(
            job.idempotency_key,
            status=JOB_AWAITING_QUALITY,
            result_paths=["/115/云下载/jav待验收/TEST-001"],
        )

        await self.service._notify(job)

        due = await self.repository.due_callbacks("9999-12-31T00:00:00+00:00")
        self.assertEqual(len(due), 1)
        self.assertEqual(due[0]["attempt_id"], 31)

    async def test_status_notifier_receives_quality_and_failure_transitions(self):
        notifier = AsyncMock()
        self.service.set_status_notifier(notifier)
        job = DownloadJob(
            idempotency_key="jav:11:candidate:21",
            batch_id=41,
            attempt_id=31,
            jav_id=11,
            candidate_id=21,
            code="TEST-001",
            magnet_uri=MAGNET,
            info_hash="0123456789abcdef0123456789abcdef01234567",
            callback_path="/jav/magnet-queue/attempts/31",
            status=JOB_AWAITING_QUALITY,
            result_paths=["/115/云下载/jav待验收/TEST-001"],
        )

        await self.service._notify(job)
        job.status = JOB_FAILED
        job.error = "CloudDrive2 离线任务失败"
        await self.service._notify(job)

        self.assertEqual(notifier.await_count, 2)
        self.assertEqual(notifier.await_args_list[0].args[0].status, JOB_AWAITING_QUALITY)
        self.assertEqual(notifier.await_args_list[1].args[0].status, JOB_FAILED)

    async def test_finished_offline_task_enters_quality_review(self):
        submitted = await self.service.submit_batch(batch_payload())
        job = await self.repository.get_job(submitted["tasks"][0]["idempotency_key"])

        await self.service._apply_offline_task(
            job,
            OfflineTask(
                name="TEST-001",
                url=MAGNET,
                info_hash=job.info_hash,
                status=clouddrive_pb2.OFFLINE_FINISHED,
                percent_done=100,
            ),
        )

        stored = await self.repository.get_job(job.idempotency_key)
        self.assertEqual(stored.status, JOB_AWAITING_QUALITY)
        self.assertEqual(stored.result_paths, ["/115/云下载/jav待验收/TEST-001"])
        self.javboss.update_download_attempt.assert_awaited_once()

    async def test_accept_moves_to_library_and_reject_deletes_staging(self):
        submitted = await self.service.submit_batch(batch_payload())
        key = submitted["tasks"][0]["idempotency_key"]
        await self.repository.update_job(
            key,
            status=JOB_AWAITING_QUALITY,
            result_paths=["/115/云下载/jav待验收/TEST-001"],
        )

        accepted = await self.service.review_attempt(31, "accepted")

        self.assertEqual(accepted.status, JOB_AWAITING_SCAN)
        self.cd2.move_from_staging.assert_awaited_once()

        second = batch_payload(key="jav:12:candidate:22", attempt_id=32)
        second["items"][0].update(jav_id=12, candidate_id=22, code="TEST-002")
        submitted = await self.service.submit_batch(second)
        key = submitted["tasks"][0]["idempotency_key"]
        await self.repository.update_job(
            key,
            status=JOB_AWAITING_QUALITY,
            result_paths=["/115/云下载/jav待验收/TEST-002"],
        )

        rejected = await self.service.review_attempt(32, "rejected")

        self.assertEqual(rejected.status, JOB_REJECTED)
        self.cd2.delete_from_staging.assert_awaited_once()

    async def test_batch_review_groups_move_and_delete_operations(self):
        first = await self.service.submit_batch(batch_payload())
        first_job = await self.repository.get_job(first["tasks"][0]["idempotency_key"])
        await self.repository.update_job(
            first_job.idempotency_key,
            status=JOB_AWAITING_QUALITY,
            result_paths=["/115/云下载/jav待验收/TEST-001"],
        )

        second_payload = batch_payload(key="jav:12:candidate:22", attempt_id=32)
        second_payload["items"][0].update(jav_id=12, candidate_id=22, code="TEST-002")
        second = await self.service.submit_batch(second_payload)
        second_job = await self.repository.get_job(second["tasks"][0]["idempotency_key"])
        await self.repository.update_job(
            second_job.idempotency_key,
            status=JOB_AWAITING_QUALITY,
            result_paths=["/115/云下载/jav待验收/TEST-002"],
        )

        await self.service.review_batch(
            [
                {"attempt_id": 31, "decision": "accepted"},
                {"attempt_id": 32, "decision": "rejected"},
            ]
        )

        self.cd2.move_from_staging.assert_awaited_once()
        self.assertEqual(
            self.cd2.move_from_staging.await_args.args[0],
            ["/115/云下载/jav待验收/TEST-001"],
        )
        self.cd2.delete_from_staging.assert_awaited_once()
        self.assertEqual(
            self.cd2.delete_from_staging.await_args.args[0],
            ["/115/云下载/jav待验收/TEST-002"],
        )
        self.assertEqual(
            (await self.repository.get_job_by_attempt(31)).status, JOB_AWAITING_SCAN
        )
        self.assertEqual(
            (await self.repository.get_job_by_attempt(32)).status, JOB_REJECTED
        )

    async def test_batch_accept_cleans_selected_task_before_move(self):
        submitted = await self.service.submit_batch(batch_payload())
        job = await self.repository.get_job(submitted["tasks"][0]["idempotency_key"])
        folder = "/115/云下载/jav待验收/TEST-001"
        await self.repository.update_job(
            job.idempotency_key,
            status=JOB_AWAITING_QUALITY,
            result_paths=[folder],
        )

        video = SimpleNamespace(
            isDirectory=False,
            fullPathName=f"{folder}/TEST-001.mkv",
            name="TEST-001.mkv",
            size=2 * 1024 * 1024 * 1024,
        )
        sidecar = SimpleNamespace(
            isDirectory=False,
            fullPathName=f"{folder}/广告.txt",
            name="广告.txt",
            size=500 * 1024 * 1024,
        )

        async def list_children(path, **_kwargs):
            if path == "/115/云下载":
                return [SimpleNamespace(name="jav待验收", isDirectory=True)]
            if path == folder:
                return [video, sidecar]
            return []

        self.cd2.list_children.side_effect = list_children

        await self.service.review_batch(
            [{"attempt_id": job.attempt_id, "decision": "accepted"}]
        )

        self.cd2.delete_from_staging.assert_awaited_once_with(
            [f"{folder}/广告.txt"], self.config.jav_staging_path
        )
        self.cd2.move_from_staging.assert_awaited_once_with(
            [folder], self.config.jav_staging_path, self.config.jav_library_path
        )

    async def test_cleanup_never_escapes_selected_task_root(self):
        folder = "/115/云下载/jav待验收/TEST-001"
        escaped = SimpleNamespace(
            isDirectory=False,
            fullPathName="/115/云下载/jav待验收/OTHER-002/other.txt",
            name="other.txt",
            size=1,
        )
        self.cd2.list_children.side_effect = AsyncMock(return_value=[escaped])

        stats = await clean_jav_paths(
            self.cd2,
            self.config.jav_staging_path,
            [folder],
            size_threshold_mb=300,
            blacklist=[],
        )

        self.assertEqual(stats.files_scanned, 0)
        self.cd2.delete_from_staging.assert_not_awaited()


class ManagedPathTest(unittest.TestCase):
    def test_move_or_delete_source_must_be_below_staging(self):
        root = "/115/云下载/jav待验收"
        self.assertEqual(
            ensure_path_below(root + "/TEST-001", root), root + "/TEST-001"
        )
        for path in (root, "/115/云下载/别的文件", "/115/正式作品库/TEST-001"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                ensure_path_below(path, root)


if __name__ == "__main__":
    unittest.main()
