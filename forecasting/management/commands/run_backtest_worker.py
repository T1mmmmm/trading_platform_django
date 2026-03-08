import json
import time
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from forecasting.models import (
    BacktestRun,
    BacktestStatus,
    ForecastJob,
    Report,
    SignalRun,
    SimAccount,
    TradeSimRun,
)
from forecasting.tasks import run_signal_job, run_trade_sim


def build_backtest_report_markdown(bt: BacktestRun, metrics: dict) -> str:
    return f"""# Backtest Report

## Run Summary
- BacktestRunId: {bt.backtest_run_id}
- DatasetVersionId: {bt.dataset_version.dataset_version_id}
- StrategyId: {bt.strategy.strategy_id}
- Forecast Config: {bt.forecast_config_snapshot_json}
- Account Config: {bt.account_config_json}
- Execution Config: {bt.execution_config_json}

## Metrics
- totalReturn: {metrics.get("totalReturn")}
- maxDrawdown: {metrics.get("maxDrawdown")}
- finalEquity: {metrics.get("finalEquity")}
- tradeCount: {metrics.get("tradeCount")}

## Interpretation
- This is a template-generated report for this lesson.
- Next step: replace this section with richer analysis/LLM output.
"""


class Command(BaseCommand):
    help = "Run backtest orchestrator worker loop"

    ACTIVE_STATUSES = [
        BacktestStatus.CREATED,
        BacktestStatus.FORECAST_PENDING,
        BacktestStatus.FORECAST_DONE,
        BacktestStatus.SIGNAL_PENDING,
        BacktestStatus.SIGNAL_DONE,
        BacktestStatus.SIM_PENDING,
        BacktestStatus.SIM_DONE,
        BacktestStatus.METRICS_DONE,
    ]

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS("Backtest worker started. Polling DB..."))

        while True:
            bt = (
                BacktestRun.objects.filter(status__in=self.ACTIVE_STATUSES)
                .order_by("created_at")
                .first()
            )
            if not bt:
                time.sleep(0.5)
                continue

            with transaction.atomic():
                bt = BacktestRun.objects.select_for_update().get(id=bt.id)
                try:
                    self._advance_one_step(bt)
                except Exception as e:
                    bt.status = BacktestStatus.FAILED
                    bt.retry_count = bt.retry_count + 1
                    bt.last_error = f"{type(e).__name__}: {e}"
                    bt.finished_at = timezone.now()
                    bt.save(
                        update_fields=[
                            "status",
                            "retry_count",
                            "last_error",
                            "finished_at",
                        ]
                    )
                    self.stderr.write(
                        f"FAILED: {bt.backtest_run_id} -> {bt.last_error}"
                    )

    def _advance_one_step(self, bt: BacktestRun) -> None:
        if bt.status == BacktestStatus.CREATED:
            self._on_created(bt)
        elif bt.status == BacktestStatus.FORECAST_PENDING:
            self._on_forecast_pending(bt)
        elif bt.status == BacktestStatus.FORECAST_DONE:
            self._on_forecast_done(bt)
        elif bt.status == BacktestStatus.SIGNAL_PENDING:
            self._on_signal_pending(bt)
        elif bt.status == BacktestStatus.SIGNAL_DONE:
            self._on_signal_done(bt)
        elif bt.status == BacktestStatus.SIM_PENDING:
            self._on_sim_pending(bt)
        elif bt.status == BacktestStatus.SIM_DONE:
            self._on_sim_done(bt)
        elif bt.status == BacktestStatus.METRICS_DONE:
            self._on_metrics_done(bt)

    def _on_created(self, bt: BacktestRun) -> None:
        cfg = bt.forecast_config_snapshot_json or {}
        params = cfg.get("params", {})
        model_type = cfg.get("modelType", "MA")
        horizon = int(cfg.get("horizon", 10))

        job = ForecastJob.objects.create(
            forecast_job_id=ForecastJob.new_job_id(),
            tenant_id=bt.tenant_id,
            dataset_version=bt.dataset_version,
            model_type=model_type,
            params_json=params,
            horizon=horizon,
            status="PENDING",
        )

        bt.forecast_job_id = job.forecast_job_id
        bt.status = BacktestStatus.FORECAST_PENDING
        bt.last_error = None
        if bt.started_at is None:
            bt.started_at = timezone.now()
            bt.save(
                update_fields=[
                    "forecast_job_id",
                    "status",
                    "last_error",
                    "started_at",
                ]
            )
        else:
            bt.save(update_fields=["forecast_job_id", "status", "last_error"])

        self.stdout.write(f"{bt.backtest_run_id}: CREATED -> FORECAST_PENDING")

    def _on_forecast_pending(self, bt: BacktestRun) -> None:
        if not bt.forecast_job_id:
            raise ValueError("backtest missing forecast_job_id")
        job = ForecastJob.objects.filter(
            tenant_id=bt.tenant_id,
            forecast_job_id=bt.forecast_job_id,
        ).first()
        if not job:
            raise ValueError("forecast job not found")

        if job.status == "SUCCEEDED":
            bt.status = BacktestStatus.FORECAST_DONE
            bt.last_error = None
            bt.save(update_fields=["status", "last_error"])
            self.stdout.write(f"{bt.backtest_run_id}: FORECAST_PENDING -> FORECAST_DONE")
        elif job.status == "FAILED":
            bt.status = BacktestStatus.FAILED
            bt.last_error = f"Forecast failed: {job.error_message}"
            bt.finished_at = timezone.now()
            bt.save(update_fields=["status", "last_error", "finished_at"])
            self.stderr.write(f"FAILED: {bt.backtest_run_id} -> {bt.last_error}")

    def _on_forecast_done(self, bt: BacktestRun) -> None:
        sr = SignalRun.objects.create(
            tenant_id=bt.tenant_id,
            forecast_job_id=bt.forecast_job_id,
            strategy=bt.strategy,
            status="PENDING",
        )
        bt.signal_run_id = sr.signal_run_id
        bt.status = BacktestStatus.SIGNAL_PENDING
        bt.last_error = None
        bt.save(update_fields=["signal_run_id", "status", "last_error"])

        try:
            run_signal_job.delay(sr.signal_run_id)
        except Exception:
            pass

        self.stdout.write(f"{bt.backtest_run_id}: FORECAST_DONE -> SIGNAL_PENDING")

    def _on_signal_pending(self, bt: BacktestRun) -> None:
        if not bt.signal_run_id:
            raise ValueError("backtest missing signal_run_id")
        sr = SignalRun.objects.filter(
            tenant_id=bt.tenant_id,
            signal_run_id=bt.signal_run_id,
        ).first()
        if not sr:
            raise ValueError("signal run not found")

        if sr.status == "SUCCEEDED":
            bt.status = BacktestStatus.SIGNAL_DONE
            bt.last_error = None
            bt.save(update_fields=["status", "last_error"])
            self.stdout.write(f"{bt.backtest_run_id}: SIGNAL_PENDING -> SIGNAL_DONE")
        elif sr.status == "FAILED":
            bt.status = BacktestStatus.FAILED
            bt.last_error = f"Signal failed: {sr.error_message}"
            bt.finished_at = timezone.now()
            bt.save(update_fields=["status", "last_error", "finished_at"])
            self.stderr.write(f"FAILED: {bt.backtest_run_id} -> {bt.last_error}")

    def _on_signal_done(self, bt: BacktestRun) -> None:
        acct_cfg = bt.account_config_json or {}
        base_currency = acct_cfg.get("baseCurrency", "USD")
        initial_cash = Decimal(str(acct_cfg.get("initialCash", 100000)))

        account = SimAccount.objects.create(
            tenant_id=bt.tenant_id,
            base_currency=base_currency,
            initial_cash=initial_cash,
        )

        execution_cfg = bt.execution_config_json or {}
        sim_run = TradeSimRun.objects.create(
            tenant_id=bt.tenant_id,
            account=account,
            signal_run=SignalRun.objects.get(signal_run_id=bt.signal_run_id),
            execution_model=execution_cfg.get("model", "NEXT_BAR_CLOSE"),
            status="PENDING",
        )
        bt.trade_sim_run_id = sim_run.trade_sim_run_id
        bt.status = BacktestStatus.SIM_PENDING
        bt.last_error = None
        bt.save(update_fields=["trade_sim_run_id", "status", "last_error"])

        try:
            run_trade_sim.delay(sim_run.trade_sim_run_id)
        except Exception:
            pass

        self.stdout.write(f"{bt.backtest_run_id}: SIGNAL_DONE -> SIM_PENDING")

    def _on_sim_pending(self, bt: BacktestRun) -> None:
        if not bt.trade_sim_run_id:
            raise ValueError("backtest missing trade_sim_run_id")
        sim_run = TradeSimRun.objects.filter(
            tenant_id=bt.tenant_id,
            trade_sim_run_id=bt.trade_sim_run_id,
        ).first()
        if not sim_run:
            raise ValueError("trade simulation run not found")

        if sim_run.status == "SUCCEEDED":
            bt.status = BacktestStatus.SIM_DONE
            bt.last_error = None
            bt.save(update_fields=["status", "last_error"])
            self.stdout.write(f"{bt.backtest_run_id}: SIM_PENDING -> SIM_DONE")
        elif sim_run.status == "FAILED":
            bt.status = BacktestStatus.FAILED
            bt.last_error = f"Simulation failed: {sim_run.error_message}"
            bt.finished_at = timezone.now()
            bt.save(update_fields=["status", "last_error", "finished_at"])
            self.stderr.write(f"FAILED: {bt.backtest_run_id} -> {bt.last_error}")

    def _on_sim_done(self, bt: BacktestRun) -> None:
        sim_run = TradeSimRun.objects.get(
            tenant_id=bt.tenant_id,
            trade_sim_run_id=bt.trade_sim_run_id,
        )
        payload = sim_run.result
        if not payload and sim_run.output_uri:
            payload = json.loads(Path(sim_run.output_uri).read_text(encoding="utf-8"))
        if not payload:
            raise ValueError("missing simulation payload")

        equity_curve = payload.get("equityCurve", [])
        fills = payload.get("fills", [])
        metrics = dict(payload.get("metrics", {}))
        if equity_curve:
            metrics["finalEquity"] = equity_curve[-1].get("equity")
        metrics["tradeCount"] = len(fills)

        result = {
            "backtestRunId": bt.backtest_run_id,
            "status": BacktestStatus.METRICS_DONE,
            "forecastJobId": bt.forecast_job_id,
            "signalRunId": bt.signal_run_id,
            "tradeSimRunId": bt.trade_sim_run_id,
            "metrics": metrics,
            "equityCurve": equity_curve,
            "reportUri": bt.report_uri,
        }

        out_dir = Path(settings.ARTIFACT_DIR) / bt.tenant_id / "backtests"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{bt.backtest_run_id}.json"
        out_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        bt.metrics_json = metrics
        bt.output_uri = str(out_path)
        bt.status = BacktestStatus.METRICS_DONE
        bt.last_error = None
        bt.save(update_fields=["metrics_json", "output_uri", "status", "last_error"])
        self.stdout.write(f"{bt.backtest_run_id}: SIM_DONE -> METRICS_DONE")

    def _on_metrics_done(self, bt: BacktestRun) -> None:
        reports_dir = Path(settings.ARTIFACT_DIR) / bt.tenant_id / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        report_path = reports_dir / f"{bt.backtest_run_id}.md"
        report_path.write_text(
            build_backtest_report_markdown(bt, bt.metrics_json or {}),
            encoding="utf-8",
        )

        Report.objects.create(
            report_id=Report.new_report_id(),
            tenant_id=bt.tenant_id,
            source_type="BACKTEST",
            source_id=bt.backtest_run_id,
            format="MARKDOWN",
            uri=str(report_path),
        )

        bt.report_uri = str(report_path)
        bt.status = BacktestStatus.REPORT_DONE
        bt.finished_at = timezone.now()
        bt.last_error = None
        bt.save(update_fields=["report_uri", "status", "finished_at", "last_error"])
        self.stdout.write(f"{bt.backtest_run_id}: METRICS_DONE -> REPORT_DONE")
