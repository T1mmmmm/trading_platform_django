import json
import random
import time
from datetime import timedelta
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from forecasting.models import ForecastJob, JobStatus

MODEL_ARTIFACT_VERSION = "stub-model:v0.1"

def write_artifact(job_id: str, horizon: int) -> Path:
    start_date = timezone.now().date()
    preds = []
    base = random.uniform(100, 500)
    for i in range(horizon):
        ts = start_date + timedelta(days=i + 1)
        base += random.uniform(-2.0, 2.0)
        preds.append({"timestamp": ts.isoformat(), "yhat": round(base, 4)})

    payload = {
        "predictions": preds,
        "metrics": {"rmse": round(random.uniform(0.5, 3.0), 4)},
        "modelArtifactVersion": MODEL_ARTIFACT_VERSION,
    }

    path = settings.ARTIFACT_DIR / f"{job_id}.json"
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

                artifact = write_artifact(job.forecast_job_id, job.horizon)
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
