import time
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from forecasting.models import BacktestRun, BacktestStatus
from llm.adapters import StubLLMAdapter
from llm.models import LLMTask, LLMTaskType, Report, JobStatus
from llm.prompt_builder import build_backtest_diagnosis_prompt, build_backtest_report_prompt


def build_backtest_summary(bt: BacktestRun) -> dict:
    metrics = bt.metrics_json or {}
    return {
        "backtestRunId": bt.backtest_run_id,
        "datasetVersionId": bt.dataset_version_id,
        "strategyId": bt.strategy_id,
        "totalReturn": metrics.get("totalReturn"),
        "maxDrawdown": metrics.get("maxDrawdown"),
        "sharpe": metrics.get("sharpe"),
        "tradeCount": metrics.get("tradeCount"),
        "winRate": metrics.get("winRate"),
    }


def build_backtest_prompt(bt: BacktestRun, task_type: str) -> str:
    summary = build_backtest_summary(bt)
    if task_type == LLMTaskType.GENERATE_REPORT:
        return build_backtest_report_prompt(summary)
    if task_type == LLMTaskType.EXPLAIN_BACKTEST:
        return build_backtest_report_prompt(summary)
    if task_type == LLMTaskType.DIAGNOSE_RESULT:
        return build_backtest_diagnosis_prompt(summary)
    raise ValueError(f"Unsupported task_type={task_type}")


def write_report(task_id: str, content: str) -> Path:
    report_dir = settings.ARTIFACT_DIR / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    path = report_dir / f"{task_id}_report.md"
    path.write_text(content, encoding="utf-8")
    return path


def claim_next_pending_task():
    with transaction.atomic():
        task = (
            LLMTask.objects.select_for_update()
            .filter(status=JobStatus.PENDING)
            .order_by("created_at")
            .first()
        )
        if not task:
            return None

        task.status = JobStatus.RUNNING
        task.started_at = timezone.now()
        task.error_message = None
        task.save(update_fields=["status", "started_at", "error_message"])
        return task


def validate_backtest_ready(bt: BacktestRun) -> None:
    allowed_statuses = {BacktestStatus.METRICS_DONE, BacktestStatus.REPORT_DONE}
    if bt.status not in allowed_statuses:
        raise ValueError(f"BacktestRun {bt.backtest_run_id} is not ready for LLM analysis")


def process_task(task: LLMTask, adapter=None) -> LLMTask:
    adapter = adapter or StubLLMAdapter()

    try:
        if task.source_type != "BACKTEST":
            raise ValueError(f"Unsupported source_type={task.source_type}")

        bt = BacktestRun.objects.get(
            backtest_run_id=task.source_id,
            tenant_id=task.tenant_id,
        )
        validate_backtest_ready(bt)

        prompt = build_backtest_prompt(bt, task.task_type)
        content = adapter.generate(prompt)
        path = write_report(task.llm_task_id, content)

        task.model_name = getattr(adapter, "model_name", task.model_name)
        task.output_uri = str(path)
        task.status = JobStatus.SUCCEEDED
        task.finished_at = timezone.now()
        task.save(update_fields=["model_name", "output_uri", "status", "finished_at"])

        Report.objects.create(
            report_id=Report.new_report_id(),
            tenant_id=task.tenant_id,
            source_type=task.source_type,
            source_id=task.source_id,
            llm_task_id=task.llm_task_id,
            title=build_report_title(task.task_type),
            format="MARKDOWN",
            uri=str(path),
            summary_text=build_report_summary(task.task_type),
        )
        return task
    except Exception as exc:
        task.status = JobStatus.FAILED
        task.error_message = f"{type(exc).__name__}: {exc}"
        task.finished_at = timezone.now()
        task.save(update_fields=["status", "error_message", "finished_at"])
        raise


def process_next_task(adapter=None):
    task = claim_next_pending_task()
    if not task:
        return None
    return process_task(task, adapter=adapter)


def build_report_title(task_type: str) -> str:
    if task_type == LLMTaskType.DIAGNOSE_RESULT:
        return "Backtest Result Diagnosis"
    if task_type == LLMTaskType.EXPLAIN_BACKTEST:
        return "Backtest Explanation"
    return "Backtest Analysis Report"


def build_report_summary(task_type: str) -> str:
    if task_type == LLMTaskType.DIAGNOSE_RESULT:
        return "AI generated diagnosis of drawdown, trade frequency, and performance concentration"
    if task_type == LLMTaskType.EXPLAIN_BACKTEST:
        return "AI generated explanation of backtest behavior"
    return "AI generated analysis report"


class Command(BaseCommand):
    help = "Run LLM worker loop"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("LLM worker started. Polling DB..."))
        adapter = StubLLMAdapter()

        while True:
            task = claim_next_pending_task()
            if not task:
                time.sleep(0.5)
                continue

            try:
                process_task(task, adapter=adapter)
                self.stdout.write(f"SUCCEEDED:{task.llm_task_id}")
            except Exception:
                task.refresh_from_db()
                self.stderr.write(f"FAILED:{task.llm_task_id} ->{task.error_message}")
