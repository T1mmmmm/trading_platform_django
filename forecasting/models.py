# Create your models here.
from django.db import models
import uuid
from django.db.models import JSONField


class BacktestStatus(models.TextChoices):
    CREATED = "CREATED"
    FORECAST_PENDING = "FORECAST_PENDING"
    FORECAST_DONE = "FORECAST_DONE"
    SIGNAL_PENDING = "SIGNAL_PENDING"
    SIGNAL_DONE = "SIGNAL_DONE"
    SIM_PENDING = "SIM_PENDING"
    SIM_DONE = "SIM_DONE"
    METRICS_DONE = "METRICS_DONE"
    REPORT_DONE = "REPORT_DONE"
    FAILED = "FAILED"

class BacktestRun(models.Model):
    backtest_run_id = models.CharField(max_length=64, unique=True, db_index=True)
    tenant_id = models.CharField(max_length=64, db_index=True)

    dataset_version = models.ForeignKey("DatasetVersion", on_delete=models.PROTECT)
    strategy = models.ForeignKey("Strategy", on_delete=models.PROTECT)

    forecast_config_snapshot_json = models.JSONField(default=dict)
    account_config_json = models.JSONField(default=dict)
    execution_config_json = models.JSONField(default=dict)
    risk_rules_json = models.JSONField(default=dict)

    status = models.CharField(max_length=32, choices=BacktestStatus.choices, default=BacktestStatus.CREATED)

    forecast_job_id = models.CharField(max_length=64, null=True, blank=True)
    signal_run_id = models.CharField(max_length=64, null=True, blank=True)
    trade_sim_run_id = models.CharField(max_length=64, null=True, blank=True)

    metrics_json = models.JSONField(default=dict)
    output_uri = models.TextField(null=True, blank=True)
    report_uri = models.TextField(null=True, blank=True)

    retry_count = models.IntegerField(default=0)
    last_error = models.TextField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    @staticmethod
    def new_backtest_run_id() -> str:
        return f"bt_{uuid.uuid4().hex[:12]}"

class Report(models.Model):
    report_id = models.CharField(max_length=64, unique=True, db_index=True)
    tenant_id = models.CharField(max_length=64, db_index=True)

    source_type = models.CharField(max_length=32)
    source_id = models.CharField(max_length=64)
    format = models.CharField(max_length=32, default="MARKDOWN")
    uri = models.TextField()

    created_at = models.DateTimeField(auto_now_add=True)

    @staticmethod
    def new_report_id() -> str:
        return f"rp_{uuid.uuid4().hex[:12]}"
    
    
class TimestampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True

class Strategy(TimestampedModel):
    strategy_id = models.CharField(max_length=64, unique=True, default=uuid.uuid4)
    tenant_id = models.CharField(max_length=64)
    name = models.CharField(max_length=128)
    type = models.CharField(max_length=32)
    spec_json = JSONField(default=dict)

    def __str__(self):
        return self.name

class SignalRun(TimestampedModel):
    signal_run_id = models.CharField(max_length=64, unique=True, default=uuid.uuid4)
    tenant_id = models.CharField(max_length=64)
    forecast_job_id = models.CharField(max_length=128)
    strategy = models.ForeignKey(Strategy, on_delete=models.CASCADE)
    status = models.CharField(max_length=32, default="PENDING")
    output_uri = models.CharField(max_length=512, blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)

class SimAccount(TimestampedModel):
    account_id = models.CharField(max_length=64, unique=True, default=uuid.uuid4)
    tenant_id = models.CharField(max_length=64)
    base_currency = models.CharField(max_length=16, default="USD")
    initial_cash = models.DecimalField(max_digits=20, decimal_places=2)

class TradeSimRun(TimestampedModel):
    trade_sim_run_id = models.CharField(max_length=64, unique=True, default=uuid.uuid4)
    tenant_id = models.CharField(max_length=64)
    account = models.ForeignKey(SimAccount, on_delete=models.CASCADE)
    signal_run = models.ForeignKey(SignalRun, on_delete=models.CASCADE)
    execution_model = models.CharField(max_length=32, default="NEXT_BAR_CLOSE")
    status = models.CharField(max_length=32, default="PENDING")
    output_uri = models.CharField(max_length=512, blank=True, null=True)
    error_message = models.TextField(blank=True, null=True)
    result = JSONField(blank=True, null=True)

class JobStatus(models.TextChoices):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

class Dataset(models.Model):
    dataset_id = models.CharField(max_length=64, unique=True, db_index=True)
    tenant_id = models.CharField(max_length=64, db_index=True)
    name = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)

    @staticmethod
    def new_dataset_id() -> str:
        return f"ds_{uuid.uuid4().hex[:10]}"

class DatasetVersionStatus(models.TextChoices):
    VALIDATING = "VALIDATING"
    READY = "READY"
    FAILED = "FAILED"

class DatasetVersion(models.Model):
    dataset_version_id = models.CharField(max_length=64, unique=True, db_index=True)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE, related_name="versions")
    tenant_id = models.CharField(max_length=64, db_index=True)

    raw_uri = models.TextField()
    processed_uri = models.TextField(null=True, blank=True)
    schema_json = models.JSONField(default=dict)

    checksum = models.CharField(max_length=128, null=True, blank=True)
    profile_json = models.JSONField(default=dict)

    status = models.CharField(max_length=16, choices=DatasetVersionStatus.choices, default=DatasetVersionStatus.VALIDATING)
    error_message = models.TextField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    @staticmethod
    def new_dataset_version_id() -> str:
        return f"dsv_{uuid.uuid4().hex[:12]}"

class ForecastJob(models.Model):
    """
    本节课最小模型：存 job 元数据 + 状态 + outputUri
    """
    dedup_key = models.CharField(max_length=128, null=True, blank=True, db_index=True)

    forecast_job_id = models.CharField(max_length=64, unique=True, db_index=True)
    tenant_id = models.CharField(max_length=64, db_index=True)
    # ✅ Forecast must bind DatasetVersion
    dataset_version = models.ForeignKey(
        DatasetVersion,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
    )
    idempotency_key = models.CharField(max_length=128, null=True, blank=True, db_index=True)

    model_type = models.CharField(max_length=64)
    params_json = models.JSONField(default=dict)
    horizon = models.IntegerField()

    status = models.CharField(max_length=16, choices=JobStatus.choices, default=JobStatus.PENDING)

    output_uri = models.TextField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["tenant_id", "idempotency_key"],
                name="uq_tenant_idem_key"
            )
        ]

    @staticmethod
    def new_job_id() -> str:
        return f"fc_{uuid.uuid4().hex[:12]}"
