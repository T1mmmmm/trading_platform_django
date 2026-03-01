import time

from django.core.management.base import BaseCommand
from django.db import transaction

from forecasting.models import SignalRun
from forecasting.tasks import run_signal_job


class Command(BaseCommand):
    help = "Run signal worker loop (poll DB for PENDING signal runs)"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Signal worker started. Polling DB..."))

        while True:
            signal_run = (
                SignalRun.objects.filter(status="PENDING")
                .order_by("created_at")
                .first()
            )
            if not signal_run:
                time.sleep(0.5)
                continue

            with transaction.atomic():
                signal_run = SignalRun.objects.select_for_update().get(id=signal_run.id)
                if signal_run.status != "PENDING":
                    continue
                signal_run.status = "RUNNING"
                signal_run.error_message = None
                signal_run.save(update_fields=["status", "error_message", "updated_at"])

            try:
                # Run inline in this worker process.
                run_signal_job(signal_run.signal_run_id)

                signal_run.refresh_from_db()
                if signal_run.status == "SUCCEEDED":
                    self.stdout.write(f"SUCCEEDED: {signal_run.signal_run_id}")
                elif signal_run.status == "FAILED":
                    self.stderr.write(
                        f"FAILED: {signal_run.signal_run_id} -> {signal_run.error_message}"
                    )
                else:
                    signal_run.status = "FAILED"
                    signal_run.error_message = (
                        f"Unexpected status after run_signal_job: {signal_run.status}"
                    )
                    signal_run.save(update_fields=["status", "error_message", "updated_at"])
                    self.stderr.write(
                        f"FAILED: {signal_run.signal_run_id} -> {signal_run.error_message}"
                    )

            except Exception as e:
                signal_run.status = "FAILED"
                signal_run.error_message = f"{type(e).__name__}: {e}"
                signal_run.save(update_fields=["status", "error_message", "updated_at"])
                self.stderr.write(
                    f"FAILED: {signal_run.signal_run_id} -> {signal_run.error_message}"
                )
