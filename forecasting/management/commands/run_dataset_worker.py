import time
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from django.conf import settings

from forecasting.models import DatasetVersion, DatasetVersionStatus
from forecasting.services.dataset_service import normalize_and_profile_csv

class Command(BaseCommand):
    help = "Run dataset worker loop (poll DB for VALIDATING dataset versions)"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Dataset worker started. Polling DB..."))

        while True:
            dsv = DatasetVersion.objects.filter(status=DatasetVersionStatus.VALIDATING).order_by("created_at").first()
            if not dsv:
                time.sleep(0.5)
                continue

            with transaction.atomic():
                dsv = DatasetVersion.objects.select_for_update().get(id=dsv.id)
                if dsv.status != DatasetVersionStatus.VALIDATING:
                    continue
                dsv.error_message = None
                dsv.save()

            try:
                raw_path = Path(dsv.raw_uri)
                ts_col = dsv.schema_json.get("timestamp")
                target_col = dsv.schema_json.get("target")

                df, profile, checksum = normalize_and_profile_csv(
                    raw_path, ts_col, target_col
                )

                # 🔥 NEW: version-isolated artifact directory
                dataset_id = dsv.dataset.dataset_id

                version_dir = (
                    Path(settings.ARTIFACT_DIR)
                    / dsv.tenant_id
                    / "datasets"
                    / dataset_id
                    / "versions"
                    / dsv.dataset_version_id
                )
                version_dir.mkdir(parents=True, exist_ok=True)

                processed_path = version_dir / "processed.csv"
                processed_path.write_text(
                    df.to_csv(index=False),
                    encoding="utf-8"
                )

                dsv.processed_uri = str(processed_path)
                dsv.profile_json = profile
                dsv.checksum = checksum
                dsv.status = DatasetVersionStatus.READY
                dsv.finished_at = timezone.now()
                dsv.save()

                self.stdout.write(f"READY: {dsv.dataset_version_id}")

            except Exception as e:
                dsv.status = DatasetVersionStatus.FAILED
                dsv.error_message = f"{type(e).__name__}: {e}"
                dsv.finished_at = timezone.now()
                dsv.save()
                self.stderr.write(
                    f"FAILED: {dsv.dataset_version_id} -> {dsv.error_message}"
                )
