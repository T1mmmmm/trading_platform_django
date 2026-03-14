from pathlib import Path

from django.conf import settings
from django.test import TestCase, override_settings
from rest_framework.test import APIClient

from forecasting.models import (
    BacktestRun,
    BacktestStatus,
    Dataset,
    DatasetVersion,
    DatasetVersionStatus,
    Strategy,
)
from llm.management.commands.run_llm_worker import process_next_task
from llm.models import JobStatus, LLMSourceType, LLMTask, LLMTaskType, Report


@override_settings(ARTIFACT_DIR=settings.BASE_DIR / "test_artifacts")
class LLMWorkerTests(TestCase):
    def setUp(self):
        self.tenant_id = "tenant_demo_1"
        self.dataset = Dataset.objects.create(
            dataset_id="ds_test",
            tenant_id=self.tenant_id,
            name="Test Dataset",
        )
        self.dataset_version = DatasetVersion.objects.create(
            dataset_version_id="dsv_test",
            dataset=self.dataset,
            tenant_id=self.tenant_id,
            raw_uri="artifacts/datasets/raw.csv",
            status=DatasetVersionStatus.READY,
        )
        self.strategy = Strategy.objects.create(
            strategy_id="strat_test",
            tenant_id=self.tenant_id,
            name="Test Strategy",
            type="RULES",
            spec_json={},
        )
        self.client = APIClient()
        self.client.credentials(HTTP_AUTHORIZATION="Bearer demo-key-1")

    def tearDown(self):
        artifact_dir = settings.ARTIFACT_DIR
        if artifact_dir.exists():
            for path in sorted(artifact_dir.rglob("*"), reverse=True):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()

    def create_backtest(self, status=BacktestStatus.METRICS_DONE):
        return BacktestRun.objects.create(
            backtest_run_id="bt_test",
            tenant_id=self.tenant_id,
            dataset_version=self.dataset_version,
            strategy=self.strategy,
            status=status,
            metrics_json={
                "totalReturn": 0.12,
                "maxDrawdown": -0.08,
                "sharpe": 1.4,
                "tradeCount": 23,
                "winRate": 0.56,
            },
        )

    def create_task(self, source_id, task_type=LLMTaskType.GENERATE_REPORT, llm_task_id="llm_test"):
        return LLMTask.objects.create(
            llm_task_id=llm_task_id,
            tenant_id=self.tenant_id,
            task_type=task_type,
            source_type=LLMSourceType.BACKTEST,
            source_id=source_id,
        )

    def test_process_next_task_marks_success_and_writes_artifact(self):
        self.create_backtest()
        task = self.create_task("bt_test")

        processed = process_next_task()

        self.assertIsNotNone(processed)
        task.refresh_from_db()
        self.assertEqual(task.status, JobStatus.SUCCEEDED)
        self.assertIsNotNone(task.started_at)
        self.assertIsNotNone(task.finished_at)
        self.assertTrue(task.output_uri)

        artifact_path = Path(task.output_uri)
        self.assertTrue(artifact_path.exists())
        self.assertIn("Backtest Analysis Report", artifact_path.read_text(encoding="utf-8"))

        report = Report.objects.get(llm_task_id=task.llm_task_id)
        self.assertEqual(report.source_id, "bt_test")
        self.assertEqual(report.uri, task.output_uri)

    def test_process_next_task_marks_failed_when_source_not_ready(self):
        self.create_backtest(status=BacktestStatus.CREATED)
        task = self.create_task("bt_test")

        with self.assertRaises(ValueError):
            process_next_task()

        task.refresh_from_db()
        self.assertEqual(task.status, JobStatus.FAILED)
        self.assertIsNotNone(task.started_at)
        self.assertIsNotNone(task.finished_at)
        self.assertIn("not ready for LLM analysis", task.error_message)
        self.assertFalse(Report.objects.filter(llm_task_id=task.llm_task_id).exists())

    def test_create_task_api_and_fetch_result(self):
        self.create_backtest()

        create_response = self.client.post(
            "/api/v1/llm/tasks",
            {
                "taskType": "GENERATE_REPORT",
                "sourceType": "BACKTEST",
                "sourceId": "bt_test",
                "promptTemplateVersion": "v1",
            },
            format="json",
        )
        self.assertEqual(create_response.status_code, 201)
        llm_task_id = create_response.data["llmTaskId"]

        task = LLMTask.objects.get(llm_task_id=llm_task_id)
        self.assertEqual(task.status, JobStatus.PENDING)
        self.assertEqual(task.input_refs_json["backtestRunId"], "bt_test")

        process_next_task()

        result_response = self.client.get(f"/api/v1/llm/tasks/{llm_task_id}/result")
        self.assertEqual(result_response.status_code, 200)
        self.assertEqual(result_response.data["llmTaskId"], llm_task_id)
        self.assertEqual(result_response.data["format"], "MARKDOWN")
        self.assertIn("Backtest Analysis Report", result_response.data["content"])
        self.assertIsNotNone(result_response.data["reportId"])

    def test_create_task_api_rejects_unready_backtest(self):
        self.create_backtest(status=BacktestStatus.CREATED)

        response = self.client.post(
            "/api/v1/llm/tasks",
            {
                "taskType": "GENERATE_REPORT",
                "sourceType": "BACKTEST",
                "sourceId": "bt_test",
            },
            format="json",
        )
        self.assertEqual(response.status_code, 409)
        self.assertIn("not ready", response.data["detail"])

    def test_same_backtest_can_generate_report_and_diagnosis(self):
        self.create_backtest()
        report_task = self.create_task(
            "bt_test",
            task_type=LLMTaskType.GENERATE_REPORT,
            llm_task_id="llm_report",
        )
        diagnosis_task = self.create_task(
            "bt_test",
            task_type=LLMTaskType.DIAGNOSE_RESULT,
            llm_task_id="llm_diag",
        )

        process_next_task()
        process_next_task()

        report_task.refresh_from_db()
        diagnosis_task.refresh_from_db()

        self.assertEqual(report_task.status, JobStatus.SUCCEEDED)
        self.assertEqual(diagnosis_task.status, JobStatus.SUCCEEDED)

        report_content = Path(report_task.output_uri).read_text(encoding="utf-8")
        diagnosis_content = Path(diagnosis_task.output_uri).read_text(encoding="utf-8")

        self.assertIn("Backtest Analysis Report", report_content)
        self.assertIn("Backtest Result Diagnosis", diagnosis_content)
        self.assertIn("drawdown", diagnosis_content.lower())
        self.assertNotEqual(report_content, diagnosis_content)

        diagnosis_report = Report.objects.get(llm_task_id=diagnosis_task.llm_task_id)
        self.assertEqual(diagnosis_report.title, "Backtest Result Diagnosis")

    def test_diagnose_result_api_returns_diagnostic_markdown(self):
        self.create_backtest()

        create_response = self.client.post(
            "/api/v1/llm/tasks",
            {
                "taskType": "DIAGNOSE_RESULT",
                "sourceType": "BACKTEST",
                "sourceId": "bt_test",
            },
            format="json",
        )
        self.assertEqual(create_response.status_code, 201)
        llm_task_id = create_response.data["llmTaskId"]

        process_next_task()

        result_response = self.client.get(f"/api/v1/llm/tasks/{llm_task_id}/result")
        self.assertEqual(result_response.status_code, 200)
        self.assertIn("Backtest Result Diagnosis", result_response.data["content"])
        self.assertIn("drawdown", result_response.data["content"].lower())
