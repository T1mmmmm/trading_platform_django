from django.db import models
import uuid


class JobStatus(models.TextChoices):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class LLMTaskType(models.TextChoices):
    GENERATE_REPORT = "GENERATE_REPORT"
    EXPLAIN_BACKTEST = "EXPLAIN_BACKTEST"
    DIAGNOSE_RESULT = "DIAGNOSE_RESULT"


class LLMSourceType(models.TextChoices):
    FORECAST = "FORECAST"
    BACKTEST = "BACKTEST"


class ReportFormat(models.TextChoices):
    MARKDOWN = "MARKDOWN"
    JSON = "JSON"


class LLMTask(models.Model):
    llm_task_id = models.CharField(max_length=64, unique=True, db_index=True)
    tenant_id = models.CharField(max_length=64, db_index=True)

    task_type = models.CharField(max_length=64, choices=LLMTaskType.choices)
    source_type = models.CharField(max_length=32, choices=LLMSourceType.choices)
    source_id = models.CharField(max_length=64)

    input_refs_json = models.JSONField(default=dict)
    prompt_template_version = models.CharField(max_length=32, default="v1")
    model_name = models.CharField(max_length=64, default="stub-llm-v1")

    status = models.CharField(max_length=16, choices=JobStatus.choices, default=JobStatus.PENDING)
    output_uri = models.TextField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    @staticmethod
    def new_task_id() -> str:
        return f"llm_{uuid.uuid4().hex[:12]}"


class Report(models.Model):
    report_id = models.CharField(max_length=64, unique=True, db_index=True)
    tenant_id = models.CharField(max_length=64, db_index=True)

    source_type = models.CharField(max_length=32, choices=LLMSourceType.choices)
    source_id = models.CharField(max_length=64)
    llm_task_id = models.CharField(max_length=64, db_index=True)

    title = models.CharField(max_length=200)
    format = models.CharField(max_length=16, choices=ReportFormat.choices)
    uri = models.TextField()
    summary_text = models.TextField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    @staticmethod
    def new_report_id() -> str:
        return f"rp_{uuid.uuid4().hex[:12]}"