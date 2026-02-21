import json
import random
import time
import csv
import pandas as pd
from typing import List, Tuple, Optional
from datetime import timedelta
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from forecasting.models import ForecastJob, JobStatus

#MODEL_ARTIFACT_VERSION = "stub-model:v0.1"

# Read from csv
#MODEL_ARTIFACT_VERSION = "ma-model:v0.1"

MODEL_ARTIFACT_VERSION = "ma-baseline:v0.1"
def write_ma_artifact(job_id: str, processed_path: Path, window: int, horizon: int) -> Path:
    df = pd.read_csv(processed_path)
    series = df["target"].dropna().tolist()
    if len(series) < window:
        raise ValueError(f"not enough data points: have={len(series)}, need window={window}")
    ma = sum(series[-window:]) / window

    start_date = timezone.now().date()
    preds = [{"timestamp": (start_date + timedelta(days=i+1)).isoformat(), "yhat": round(float(ma), 4)} for i in range(horizon)]

    payload = {"predictions": preds, "metrics": {"rmse": None}, "modelArtifactVersion": MODEL_ARTIFACT_VERSION}
    out_path = processed_path.parent / f"{job_id}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path

def read_series_from_csv(csv_path: str, target_column: str = "Close") -> Tuple[List[str], List[float]]:
    """
    Supports:
    1) timestamp,value
    2) timestamp,...,Close,... (OHLCV) -> uses target_column (default Close)
    """
    timestamps: List[str] = []
    values: List[float] = []

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV missing header")

        fields = set(reader.fieldnames)

        if "timestamp" not in fields:
            raise ValueError("CSV must contain 'timestamp' column")

        # choose value column
        if "value" in fields:
            value_col = "value"
        elif target_column in fields:
            value_col = target_column
        elif "Close" in fields:
            value_col = "Close"
        else:
            raise ValueError("CSV must contain 'value' or a target column like 'Close'")

        for row in reader:
            ts = row.get("timestamp")
            v = row.get(value_col)
            if not ts or v is None:
                continue
            try:
                fv = float(v)
            except ValueError:
                continue
            timestamps.append(ts)
            values.append(fv)

    if len(values) < 2:
        raise ValueError("Not enough data points in CSV")

    return timestamps, values


def mean(nums: List[float]) -> float:
    return sum(nums) / len(nums)


def compute_rmse(values: List[float], window: int) -> float:
    """
    Walk-forward MA RMSE:
    predict t using mean(values[t-window : t]) for t >= window
    compare with actual values[t]
    """
    if window < 1:
        raise ValueError("window must be >= 1")
    if len(values) <= window:
        raise ValueError(f"Need > window data points for RMSE. got n={len(values)}, window={window}")

    se_sum = 0.0
    count = 0
    for t in range(window, len(values)):
        pred = mean(values[t - window : t])
        err = pred - values[t]
        se_sum += err * err
        count += 1

    return (se_sum / count) ** 0.5


def write_artifact_for_job(job: ForecastJob) -> Path:
    """
    Reads CSV, computes MA baseline, outputs horizon predictions + RMSE.
    job.params_json expects:
      - csvPath (required): "data/prices.csv"
      - window (optional): default 20
      - targetColumn (optional): default "Close"
    """
    params = job.params_json or {}
    csv_path = params.get("csvPath")
    if not csv_path:
        raise ValueError("Missing params.csvPath (e.g. 'data/prices.csv')")

    window = int(params.get("window", 20))
    target_col = params.get("targetColumn", "Close")

    ts, values = read_series_from_csv(csv_path, target_column=target_col)

    # baseline = MA of last window points
    if len(values) < window:
        raise ValueError(f"Not enough points for window={window}. got n={len(values)}")

    baseline = mean(values[-window:])
    rmse = compute_rmse(values, window)

    # forecast horizon days after last timestamp (simple: use today + i, like stub)
    start_date = timezone.now().date()
    preds = []
    for i in range(job.horizon):
        pred_ts = start_date + timedelta(days=i + 1)
        preds.append({"timestamp": pred_ts.isoformat(), "yhat": round(baseline, 4)})

    payload = {
        "predictions": preds,
        "metrics": {"rmse": round(rmse, 4)},
        "modelArtifactVersion": MODEL_ARTIFACT_VERSION,
    }

    path = settings.ARTIFACT_DIR / f"{job.forecast_job_id}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


class Command(BaseCommand):
    help = "Run forecast worker loop (poll DB for PENDING jobs)"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Forecast worker started. Polling DB..."))

        while True:
            # 1) 拉一个 PENDING job（最简实现：轮询）
            job = ForecastJob.objects.filter(status=JobStatus.PENDING).order_by("created_at").first()

            if not job:
                time.sleep(0.5)
                continue

            # 2) 置为 RUNNING（用事务避免并发重复消费）
            with transaction.atomic():
                job = ForecastJob.objects.select_for_update().get(id=job.id)
                if job.status != JobStatus.PENDING:
                    continue
                job.status = JobStatus.RUNNING
                job.started_at = timezone.now()
                job.error_message = None
                job.save()

            try:
                # 模拟耗时计算
                time.sleep(1.0)

                # ✅ new: MA from datasetVersion.processed.csv
                if not job.dataset_version:
                    raise ValueError("job missing dataset_version")
                if not job.dataset_version.processed_uri:
                    raise ValueError("datasetVersion missing processed_uri")

                processed_path = Path(job.dataset_version.processed_uri)

                window = int((job.params_json or {}).get("window", 20))
                horizon = int(job.horizon)

                artifact = write_ma_artifact(job.forecast_job_id, processed_path, window, horizon)

                job.output_uri = str(artifact)
                job.status = JobStatus.SUCCEEDED
                job.finished_at = timezone.now()
                job.save()


                self.stdout.write(f"SUCCEEDED: {job.forecast_job_id}")

            except Exception as e:
                job.status = JobStatus.FAILED
                job.error_message = f"{type(e).__name__}: {e}"
                job.finished_at = timezone.now()
                job.save()

                self.stderr.write(f"FAILED: {job.forecast_job_id} -> {job.error_message}")
