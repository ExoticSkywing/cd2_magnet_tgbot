import unittest

from main import CleanupSummary, FolderCleanupResult, format_cleanup_report


class CleanupReportTest(unittest.TestCase):
    def test_report_puts_totals_before_folder_details(self):
        summary = CleanupSummary()
        summary.add(
            FolderCleanupResult(
                folder_name="任务 A",
                files_scanned=5,
                directories_scanned=2,
                files_deleted=3,
                small_files_deleted=2,
                blacklist_files_deleted=1,
                files_kept=2,
                folders_deleted=1,
            )
        )

        report = format_cleanup_report(summary)

        self.assertLess(report.index("扫描文件：5 个"), report.index("明细："))
        self.assertIn("任务文件夹：1 个", report)
        self.assertIn("扫描目录：2 个", report)
        self.assertIn("删除文件：3 个（小文件 2，黑名单 1）", report)
        self.assertIn("删除文件夹：1 个", report)
        self.assertIn("任务 A", report)

    def test_empty_report_explains_that_no_cleanup_is_needed(self):
        report = format_cleanup_report(CleanupSummary())

        self.assertIn("任务文件夹：0 个", report)
        self.assertIn("扫描文件：0 个", report)
        self.assertIn("结果：无需清理。", report)

    def test_report_keeps_totals_when_details_are_truncated(self):
        summary = CleanupSummary()
        for index in range(20):
            summary.add(
                FolderCleanupResult(
                    folder_name=f"任务-{index}-" + ("很长名称" * 20),
                    files_scanned=1,
                    files_deleted=1,
                    small_files_deleted=1,
                )
            )

        report = format_cleanup_report(summary, max_length=500)

        self.assertLessEqual(len(report), 500)
        self.assertIn("任务文件夹：20 个", report)
        self.assertIn("其余", report)


if __name__ == "__main__":
    unittest.main()
