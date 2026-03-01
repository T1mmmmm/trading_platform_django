import json
import traceback
from pathlib import Path

import pandas as pd
from celery import shared_task
from django.conf import settings

from .models import ForecastJob, SignalRun, TradeSimRun

@shared_task
def run_signal_job(signal_run_id):
    sr = SignalRun.objects.get(signal_run_id=signal_run_id)

    try:
        if sr.status == "PENDING":
            sr.status = "RUNNING"
            sr.error_message = None
            sr.save(update_fields=["status", "error_message", "updated_at"])

        job = ForecastJob.objects.get(
            forecast_job_id=sr.forecast_job_id,
            tenant_id=sr.tenant_id,
        )
        if job.status != "SUCCEEDED":
            raise ValueError(f"ForecastJob not ready: status={job.status}")
        if not job.output_uri:
            raise ValueError("ForecastJob missing output_uri")
        if not job.dataset_version or not job.dataset_version.processed_uri:
            raise ValueError("ForecastJob missing dataset_version.processed_uri")

        with open(job.output_uri, "r", encoding="utf-8") as f:
            forecast_payload = json.load(f)

        preds = forecast_payload.get("predictions", [])
        if not preds:
            raise ValueError("Forecast artifact has no predictions")

        df_processed = pd.read_csv(job.dataset_version.processed_uri)
        if "target" not in df_processed.columns:
            raise ValueError("processed.csv missing 'target' column")
        last_price = float(df_processed["target"].dropna().iloc[-1])

        buy_pct = sr.strategy.spec_json.get("buyAbovePct", 0.0)
        sell_pct = sr.strategy.spec_json.get("sellBelowPct", 0.0)

        signals = []
        for r in preds:
            ts = r.get("timestamp")
            yhat = float(r.get("yhat"))
            act = "HOLD"
            reason = "within_band"
            if yhat >= last_price * (1 + buy_pct):
                act = "BUY"
                reason = "threshold_up"
            elif yhat <= last_price * (1 - sell_pct):
                act = "SELL"
                reason = "threshold_down"

            signals.append({
                "timestamp": ts,
                "action": act,
                "reason": reason
            })

        out_dir = Path(settings.ARTIFACT_DIR) / sr.tenant_id / "signals"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{signal_run_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({"signalRunId": signal_run_id, "signals": signals}, f, ensure_ascii=False)

        sr.output_uri = str(out_path)
        sr.status = "SUCCEEDED"
        sr.save(update_fields=["output_uri", "status", "updated_at"])

    except Exception:
        sr.status = "FAILED"
        sr.error_message = traceback.format_exc()
        sr.save(update_fields=["status", "error_message", "updated_at"])

        
@shared_task
def run_trade_sim(trade_sim_run_id):
    sim_run = TradeSimRun.objects.get(trade_sim_run_id=trade_sim_run_id)
    sr = sim_run.signal_run
    sa = sim_run.account

    try:
        if sim_run.status == "PENDING":
            sim_run.status = "RUNNING"
            sim_run.error_message = None
            sim_run.save(update_fields=["status", "error_message", "updated_at"])

        if sr.status != "SUCCEEDED":
            raise ValueError(f"SignalRun not ready: status={sr.status}")
        if not sr.output_uri:
            raise ValueError("SignalRun missing output_uri")

        job = ForecastJob.objects.get(
            forecast_job_id=sr.forecast_job_id,
            tenant_id=sim_run.tenant_id,
        )
        if not job.dataset_version or not job.dataset_version.processed_uri:
            raise ValueError("ForecastJob missing dataset_version.processed_uri")

        with open(sr.output_uri, "r", encoding="utf-8") as f:
            sig_data = json.load(f)

        df_hist = pd.read_csv(job.dataset_version.processed_uri)
        if "timestamp" not in df_hist.columns:
            raise ValueError("processed.csv missing 'timestamp' column")
        price_col = "target" if "target" in df_hist.columns else "close"
        if price_col not in df_hist.columns:
            raise ValueError("processed.csv missing price column ('target' or 'close')")

        cash = float(sa.initial_cash)
        initial_cash = cash
        shares = 0

        orders = []
        fills = []
        equity_curve = []

        for s in sig_data.get("signals", []):
            ts = s.get("timestamp")
            action = s.get("action")

            price_row = df_hist[df_hist["timestamp"] == ts]
            if price_row.empty:
                continue

            price = float(price_row.iloc[0][price_col])
            if price <= 0:
                continue

            if action == "BUY" and cash >= price:
                qty = int((cash * 0.2) // price)
                if qty > 0:
                    cash -= qty * price
                    shares += qty
                    orders.append({"timestamp": ts, "action": "BUY", "price": price, "qty": qty})
                    fills.append({"timestamp": ts, "fill_price": price, "filled_qty": qty})

            if action == "SELL" and shares > 0:
                qty = shares
                cash += qty * price
                orders.append({"timestamp": ts, "action": "SELL", "price": price, "qty": qty})
                fills.append({"timestamp": ts, "fill_price": price, "filled_qty": qty})
                shares = 0

            equity_curve.append({"timestamp": ts, "equity": cash + shares * price})

        if not equity_curve:
            equity_curve.append({"timestamp": None, "equity": initial_cash})

        final_equity = float(equity_curve[-1]["equity"])
        total_return = (final_equity / initial_cash) - 1

        peak = float(equity_curve[0]["equity"])
        max_drawdown = 0.0
        for point in equity_curve:
            eq = float(point["equity"])
            if eq > peak:
                peak = eq
            dd = (eq / peak) - 1 if peak > 0 else 0.0
            if dd < max_drawdown:
                max_drawdown = dd

        result = {
            "orders": orders,
            "fills": fills,
            "equityCurve": equity_curve,
            "metrics": {
                "totalReturn": total_return,
                "maxDrawdown": max_drawdown,
            },
        }

        out_dir = Path(settings.ARTIFACT_DIR) / sim_run.tenant_id / "sim"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{sim_run.trade_sim_run_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)

        sim_run.output_uri = str(out_path)
        sim_run.result = result
        sim_run.status = "SUCCEEDED"
        sim_run.save(update_fields=["output_uri", "result", "status", "updated_at"])

    except Exception:
        sim_run.status = "FAILED"
        sim_run.error_message = traceback.format_exc()
        sim_run.save(update_fields=["status", "error_message", "updated_at"])
