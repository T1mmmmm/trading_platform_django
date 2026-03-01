import time

from django.core.management.base import BaseCommand
from django.db import transaction

from forecasting.models import TradeSimRun
from forecasting.tasks import run_trade_sim


class Command(BaseCommand):
    help = "Run simulation worker loop (poll DB for PENDING simulation runs)"

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Simulation worker started. Polling DB..."))

        while True:
            sim_run = (
                TradeSimRun.objects.filter(status="PENDING")
                .order_by("created_at")
                .first()
            )
            if not sim_run:
                time.sleep(0.5)
                continue

            with transaction.atomic():
                sim_run = TradeSimRun.objects.select_for_update().get(id=sim_run.id)
                if sim_run.status != "PENDING":
                    continue
                sim_run.status = "RUNNING"
                sim_run.error_message = None
                sim_run.save(update_fields=["status", "error_message", "updated_at"])

            try:
                run_trade_sim(sim_run.trade_sim_run_id)
                sim_run.refresh_from_db()
                if sim_run.status == "SUCCEEDED":
                    self.stdout.write(f"SUCCEEDED: {sim_run.trade_sim_run_id}")
                elif sim_run.status == "FAILED":
                    self.stderr.write(
                        f"FAILED: {sim_run.trade_sim_run_id} -> {sim_run.error_message}"
                    )
                else:
                    sim_run.status = "FAILED"
                    sim_run.error_message = (
                        f"Unexpected status after run_trade_sim: {sim_run.status}"
                    )
                    sim_run.save(update_fields=["status", "error_message", "updated_at"])
                    self.stderr.write(
                        f"FAILED: {sim_run.trade_sim_run_id} -> {sim_run.error_message}"
                    )

            except Exception as e:
                sim_run.status = "FAILED"
                sim_run.error_message = f"{type(e).__name__}: {e}"
                sim_run.save(update_fields=["status", "error_message", "updated_at"])
                self.stderr.write(
                    f"FAILED: {sim_run.trade_sim_run_id} -> {sim_run.error_message}"
                )
