from django.shortcuts import render
from .dedup import file_checksum_sha256, normalize_params, build_dedup_key


# Create your views here.
import json
import uuid

from django.utils import timezone
from pathlib import Path
from django.conf import settings
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework import viewsets


from .models import Dataset, DatasetVersion, DatasetVersionStatus, ForecastJob, JobStatus, Strategy, SimAccount, SignalRun, TradeSimRun, BacktestRun, BacktestStatus, Report
from .tasks import run_signal_job, run_trade_sim
from .serializers import (
    DatasetCreateSerializer, DatasetCreateResponseSerializer,
    DatasetCommitSerializer, DatasetCommitResponseSerializer,
    DatasetUploadSerializer, DatasetUploadResponseSerializer,
    DatasetVersionSerializer,
    ForecastCreateSerializer,
    ForecastCreateResponseSerializer,
    ForecastJobSerializer,
    ForecastResultSerializer,
    SignalRunSerializer,
    TradeSimRunCreateSerializer,
    TradeSimRunCreateResponseSerializer,
    TradeSimRunSerializer,
    TradeSimResultSerializer,
    StrategySerializer,
    SimAccountSerializer,
    BacktestCreateSerializer,
    BacktestCreateResponseSerializer,
    BacktestDetailSerializer,
    ReportCreateSerializer,
    ReportSerializer,
)

class BacktestResultView(APIView):
    def get(self, request, backtest_run_id: str):
        tenant_id = getattr(request.user, "tenant_id", None)
        if not tenant_id:
            return Response(
                {"detail": "Authenticated user with tenant_id is required"},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        bt = BacktestRun.objects.filter(
            tenant_id=tenant_id,
            backtest_run_id=backtest_run_id,
        ).first()
        if not bt:
            return Response({"detail": "BacktestRun not found"}, status=404)

        if bt.status not in ["METRICS_DONE", "REPORT_DONE"]:
            return Response(
                {"detail": f"Backtest not ready, status={bt.status}"},
                status=409
            )

        if not bt.output_uri:
            return Response({"detail": "Missing output_uri"}, status=500)

        payload = json.loads(Path(bt.output_uri).read_text(encoding="utf-8"))
        return Response(payload, status=200)
    
class BacktestCreateView(APIView):
    def post(self, request):
        tenant_id = getattr(request.user, "tenant_id", None)
        if not tenant_id:
            return Response(
                {"detail": "Authenticated user with tenant_id is required"},
                status=status.HTTP_401_UNAUTHORIZED,
            )

        ser = BacktestCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        dsv = DatasetVersion.objects.filter(
            tenant_id=tenant_id,
            dataset_version_id=data["datasetVersionId"],
            status="READY",
        ).first()
        if not dsv:
            return Response({"detail": "DatasetVersion not found or not READY"}, status=404)

        strategy = Strategy.objects.filter(
            tenant_id=tenant_id,
            strategy_id=data["strategyId"],
        ).first()
        if not strategy:
            return Response({"detail": "Strategy not found"}, status=404)

        bt = BacktestRun.objects.create(
            backtest_run_id=BacktestRun.new_backtest_run_id(),
            tenant_id=tenant_id,
            dataset_version=dsv,
            strategy=strategy,
            forecast_config_snapshot_json=data["forecast"],
            account_config_json=data["account"],
            execution_config_json=data.get("execution", {}),
            risk_rules_json=data.get("riskRules", {}),
            status=BacktestStatus.CREATED,
        )

        return Response(
            BacktestCreateResponseSerializer({
                "backtestRunId": bt.backtest_run_id,
                "status": bt.status,
            }).data,
            status=status.HTTP_201_CREATED
        )

class BacktestDetailView(APIView):
    def get(self, request, backtest_run_id: str):
        tenant_id = getattr(request.user, "tenant_id", None)
        if not tenant_id:
            return Response(
                {"detail": "Authenticated user with tenant_id is required"},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        bt = BacktestRun.objects.filter(
            tenant_id=tenant_id,
            backtest_run_id=backtest_run_id,
        ).first()
        if not bt:
            return Response({"detail": "BacktestRun not found"}, status=404)

        out = {
            "backtestRunId": bt.backtest_run_id,
            "status": bt.status,
            "datasetVersionId": bt.dataset_version.dataset_version_id,
            "forecastJobId": bt.forecast_job_id,
            "signalRunId": bt.signal_run_id,
            "tradeSimRunId": bt.trade_sim_run_id,
            "metrics": bt.metrics_json or {},
            "reportUri": bt.report_uri,
            "lastError": bt.last_error,
        }
        return Response(BacktestDetailSerializer(out).data, status=200)


def _build_backtest_report_markdown(bt: BacktestRun, metrics: dict) -> str:
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

## Interpretation
- This is a template-generated report for this lesson.
- Next step: replace this section with richer analysis/LLM output.
"""


class ReportCreateView(APIView):
    def post(self, request):
        tenant_id = getattr(request.user, "tenant_id", None)
        if not tenant_id:
            return Response(
                {"detail": "Authenticated user with tenant_id is required"},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        ser = ReportCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        source_type = data["sourceType"]
        source_id = data["sourceId"]
        fmt = data.get("format", "MARKDOWN")

        if source_type != "BACKTEST":
            return Response({"detail": "Only sourceType=BACKTEST is supported"}, status=400)

        bt = BacktestRun.objects.filter(
            tenant_id=tenant_id,
            backtest_run_id=source_id,
        ).first()
        if not bt:
            return Response({"detail": "BacktestRun not found"}, status=404)

        metrics = bt.metrics_json or {}
        if not metrics and bt.output_uri:
            try:
                payload = json.loads(Path(bt.output_uri).read_text(encoding="utf-8"))
                metrics = payload.get("metrics", {})
            except Exception:
                metrics = {}

        reports_dir = Path(settings.ARTIFACT_DIR) / tenant_id / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        if fmt == "MARKDOWN":
            report_path = reports_dir / f"{bt.backtest_run_id}.md"
            report_path.write_text(
                _build_backtest_report_markdown(bt, metrics),
                encoding="utf-8",
            )
        elif fmt == "JSON":
            report_path = reports_dir / f"{bt.backtest_run_id}.json"
            report_path.write_text(
                json.dumps(
                    {
                        "backtestRunId": bt.backtest_run_id,
                        "datasetVersionId": bt.dataset_version.dataset_version_id,
                        "strategyId": bt.strategy.strategy_id,
                        "metrics": metrics,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        else:
            return Response({"detail": "Unsupported report format"}, status=400)

        report = Report.objects.create(
            report_id=Report.new_report_id(),
            tenant_id=tenant_id,
            source_type=source_type,
            source_id=source_id,
            format=fmt,
            uri=str(report_path),
        )

        if fmt == "MARKDOWN":
            bt.report_uri = str(report_path)
            bt.save(update_fields=["report_uri"])

        out = {
            "reportId": report.report_id,
            "sourceType": report.source_type,
            "sourceId": report.source_id,
            "format": report.format,
            "uri": report.uri,
        }
        return Response(ReportSerializer(out).data, status=201)


class ReportDetailView(APIView):
    def get(self, request, report_id: str):
        tenant_id = getattr(request.user, "tenant_id", None)
        if not tenant_id:
            return Response(
                {"detail": "Authenticated user with tenant_id is required"},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        report = Report.objects.filter(
            tenant_id=tenant_id,
            report_id=report_id,
        ).first()
        if not report:
            return Response({"detail": "Report not found"}, status=404)

        out = {
            "reportId": report.report_id,
            "sourceType": report.source_type,
            "sourceId": report.source_id,
            "format": report.format,
            "uri": report.uri,
        }
        return Response(ReportSerializer(out).data, status=200)

class SignalRunStartView(APIView):
    def post(self, request):
        tenant_id = getattr(request.user, "tenant_id", None)
        forecast_job_id = request.data.get("forecast_job_id")
        strategy_id = request.data.get("strategy_id")

        if not tenant_id:
            return Response(
                {"detail": "Authenticated user with tenant_id is required"},
                status=status.HTTP_401_UNAUTHORIZED,
            )

        if not forecast_job_id or not strategy_id:
            return Response(
                {"detail": "forecast_job_id and strategy_id are required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # validate strategy exists
        try:
            strategy = Strategy.objects.get(strategy_id=strategy_id, tenant_id=tenant_id)
        except Strategy.DoesNotExist:
            return Response({"detail": "Strategy not found"}, status=status.HTTP_404_NOT_FOUND)

        # validate forecast job exists (adjust field name to your model)
        try:
            ForecastJob.objects.get(forecast_job_id=forecast_job_id, tenant_id=tenant_id)
        except ForecastJob.DoesNotExist:
            return Response({"detail": "ForecastJob not found"}, status=status.HTTP_404_NOT_FOUND)

        sr = SignalRun.objects.create(
            tenant_id=tenant_id,
            forecast_job_id=forecast_job_id,
            strategy=strategy,
            status="PENDING",
        )
        # enqueue async job
        try:
            run_signal_job.delay(sr.signal_run_id)
        except Exception:
            # Broker may be unavailable in local dev; keep PENDING for polling worker.
            pass

        return Response(
            {"signalRunId": sr.signal_run_id, "status": sr.status},
            status=status.HTTP_201_CREATED
        )

class SignalRunDetailView(APIView):
    def get(self, request, signal_run_id: str):
        tenant_id = getattr(request.user, "tenant_id", None)
        sr = SignalRun.objects.filter(
            tenant_id=tenant_id,
            signal_run_id=signal_run_id,
        ).first()
        if not sr:
            return Response({"detail": "SignalRun not found"}, status=404)

        out = {
            "signalRunId": sr.signal_run_id,
            "status": sr.status,
            "forecastJobId": sr.forecast_job_id,
            "strategyId": sr.strategy.strategy_id,
            "createdAt": sr.created_at.isoformat(),
            "outputUri": sr.output_uri,
            "errorMessage": sr.error_message,
        }
        return Response(SignalRunSerializer(out).data, status=200)


class SignalRunResultView(APIView):
    def get(self, request, signal_run_id: str):
        tenant_id = getattr(request.user, "tenant_id", None)
        sr = SignalRun.objects.filter(
            tenant_id=tenant_id,
            signal_run_id=signal_run_id,
        ).first()
        if not sr:
            return Response({"detail": "SignalRun not found"}, status=404)
        if sr.status != "SUCCEEDED":
            return Response(
                {"detail": f"SignalRun not ready, status={sr.status}"},
                status=status.HTTP_409_CONFLICT,
            )
        if not sr.output_uri:
            return Response({"detail": "Missing signal output_uri"}, status=500)

        try:
            with open(sr.output_uri, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as e:
            return Response({"detail": f"Failed to load artifact: {e}"}, status=500)

        return Response(payload, status=200)


class SimAccountViewSet(viewsets.ModelViewSet):
    serializer_class = SimAccountSerializer

    def get_queryset(self):
        return SimAccount.objects.filter(tenant_id=self.request.user.tenant_id)

    def perform_create(self, serializer):
        serializer.save(tenant_id=self.request.user.tenant_id)

class StrategyViewSet(viewsets.ModelViewSet):
    serializer_class = StrategySerializer

    def get_queryset(self):
        return Strategy.objects.filter(tenant_id=self.request.user.tenant_id)

    def perform_create(self, serializer):
        serializer.save(tenant_id=self.request.user.tenant_id)


class TradeSimRunCreateView(APIView):
    def post(self, request):
        tenant_id = getattr(request.user, "tenant_id", None)
        if not tenant_id:
            return Response(
                {"detail": "Authenticated user with tenant_id is required"},
                status=status.HTTP_401_UNAUTHORIZED,
            )

        ser = TradeSimRunCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        account = SimAccount.objects.filter(
            tenant_id=tenant_id,
            account_id=data["account_id"],
        ).first()
        if not account:
            return Response({"detail": "SimAccount not found"}, status=404)

        signal_run = SignalRun.objects.filter(
            tenant_id=tenant_id,
            signal_run_id=data["signal_run_id"],
        ).first()
        if not signal_run:
            return Response({"detail": "SignalRun not found"}, status=404)
        if signal_run.status != "SUCCEEDED":
            return Response(
                {"detail": f"SignalRun not ready, status={signal_run.status}"},
                status=status.HTTP_409_CONFLICT,
            )

        sim_run = TradeSimRun.objects.create(
            tenant_id=tenant_id,
            account=account,
            signal_run=signal_run,
            execution_model=data.get("execution_model", "NEXT_BAR_CLOSE"),
            status="PENDING",
        )

        try:
            run_trade_sim.delay(sim_run.trade_sim_run_id)
        except Exception:
            # Broker may be unavailable in local dev; keep PENDING for polling worker.
            pass

        return Response(
            TradeSimRunCreateResponseSerializer(
                {"tradeSimRunId": sim_run.trade_sim_run_id, "status": sim_run.status}
            ).data,
            status=201,
        )


class TradeSimRunDetailView(APIView):
    def get(self, request, trade_sim_run_id: str):
        tenant_id = getattr(request.user, "tenant_id", None)
        sim_run = TradeSimRun.objects.filter(
            tenant_id=tenant_id,
            trade_sim_run_id=trade_sim_run_id,
        ).first()
        if not sim_run:
            return Response({"detail": "TradeSimRun not found"}, status=404)

        out = {
            "tradeSimRunId": sim_run.trade_sim_run_id,
            "status": sim_run.status,
            "executionModel": sim_run.execution_model,
            "createdAt": sim_run.created_at.isoformat(),
            "outputUri": sim_run.output_uri,
            "errorMessage": sim_run.error_message,
        }
        return Response(TradeSimRunSerializer(out).data, status=200)


class TradeSimRunResultView(APIView):
    def get(self, request, trade_sim_run_id: str):
        tenant_id = getattr(request.user, "tenant_id", None)
        sim_run = TradeSimRun.objects.filter(
            tenant_id=tenant_id,
            trade_sim_run_id=trade_sim_run_id,
        ).first()
        if not sim_run:
            return Response({"detail": "TradeSimRun not found"}, status=404)
        if sim_run.status != "SUCCEEDED":
            return Response(
                {"detail": f"TradeSimRun not ready, status={sim_run.status}"},
                status=status.HTTP_409_CONFLICT,
            )

        payload = sim_run.result
        if not payload and sim_run.output_uri:
            try:
                with open(sim_run.output_uri, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            except Exception as e:
                return Response({"detail": f"Failed to load artifact: {e}"}, status=500)

        if not payload:
            return Response({"detail": "Missing simulation result"}, status=500)

        return Response(TradeSimResultSerializer(payload).data, status=200)

# HW2:
class DatasetUploadView(APIView):
    parser_classes = [MultiPartParser, FormParser]

    def post(self, request, dataset_id: str):
        tenant_id = request.user.tenant_id
        ds = Dataset.objects.filter(tenant_id=tenant_id, dataset_id=dataset_id).first()
        if not ds:
            return Response({"detail": "Dataset not found"}, status=404)

        ser = DatasetUploadSerializer(data=request.data)
        ser.is_valid(raise_exception=True)

        upload_file = ser.validated_data["file"]
        mapping = ser.validated_data["columnMapping"]  # already parsed to dict by validate_columnMapping

        upload_id = f"up_{uuid.uuid4().hex[:12]}"
        raw_dir = settings.ARTIFACT_DIR / tenant_id / "datasets" / dataset_id / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        raw_path = raw_dir / f"{upload_id}.csv"

        # stream write (safe for large files)
        with raw_path.open("wb") as f:
            for chunk in upload_file.chunks():
                f.write(chunk)

        dsv_id = DatasetVersion.new_dataset_version_id()

        dsv = DatasetVersion.objects.create(
            dataset_version_id=dsv_id,
            dataset=ds,
            tenant_id=tenant_id,
            raw_uri=str(raw_path),
            schema_json={
                "timestamp": mapping.get("timestamp"),
                "target": mapping.get("target"),
            },
            status=DatasetVersionStatus.VALIDATING,
        )

        return Response(
            DatasetUploadResponseSerializer({
                "datasetVersionId": dsv.dataset_version_id,
                "status": dsv.status,
                "rawUri": dsv.raw_uri,
            }).data,
            status=201
        )

class DatasetCreateView(APIView):
    def post(self, request):
        tenant_id = request.user.tenant_id
        ser = DatasetCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)

        ds = Dataset.objects.create(
            dataset_id=Dataset.new_dataset_id(),
            tenant_id=tenant_id,
            name=ser.validated_data["name"],
        )
        return Response(DatasetCreateResponseSerializer({"datasetId": ds.dataset_id}).data, status=201)

class DatasetCommitView(APIView):
    def post(self, request, dataset_id: str):
        tenant_id = request.user.tenant_id
        ds = Dataset.objects.filter(tenant_id=tenant_id, dataset_id=dataset_id).first()
        if not ds:
            return Response({"detail": "Dataset not found"}, status=404)

        ser = DatasetCommitSerializer(data=request.data)
        ser.is_valid(raise_exception=True)

        local_path = ser.validated_data["localPath"]
        mapping = ser.validated_data["columnMapping"]

        raw_src = (settings.BASE_DIR / local_path).resolve()
        if not raw_src.exists():
            return Response({"detail": f"localPath not found: {local_path}"}, status=400)

        dsv_id = DatasetVersion.new_dataset_version_id()

        tenant_dir = settings.ARTIFACT_DIR / tenant_id / "datasets" / dsv_id
        tenant_dir.mkdir(parents=True, exist_ok=True)

        raw_uri = tenant_dir / "raw.csv"
        raw_uri.write_bytes(raw_src.read_bytes())

        dsv = DatasetVersion.objects.create(
            dataset_version_id=dsv_id,
            dataset=ds,
            tenant_id=tenant_id,
            raw_uri=str(raw_uri),
            schema_json={
                "timestamp": mapping.get("timestamp"),
                "target": mapping.get("target"),
            },
            status=DatasetVersionStatus.VALIDATING,
        )

        return Response(
            DatasetCommitResponseSerializer({"datasetVersionId": dsv.dataset_version_id, "status": dsv.status}).data,
            status=201
        )

class DatasetVersionDetailView(APIView):
    def get(self, request, dataset_id: str, dataset_version_id: str):
        tenant_id = request.user.tenant_id
        dsv = DatasetVersion.objects.filter(
            tenant_id=tenant_id,
            dataset__dataset_id=dataset_id,
            dataset_version_id=dataset_version_id
        ).first()
        if not dsv:
            return Response({"detail": "DatasetVersion not found"}, status=404)

        out = {
            "datasetVersionId": dsv.dataset_version_id,
            "status": dsv.status,
            "checksum": dsv.checksum,
            "schema": dsv.schema_json,
            "profile": dsv.profile_json or {},
            "rawUri": dsv.raw_uri,
            "processedUri": dsv.processed_uri,
            "errorMessage": dsv.error_message,
        }
        return Response(DatasetVersionSerializer(out).data, status=200)
class HealthView(APIView):
    authentication_classes = []
    permission_classes = []

    def get(self, request):
        return Response({"status": "ok"})

class ForecastListCreateView(APIView):
    def post(self, request):
        tenant_id = request.user.tenant_id
        idem_key = request.headers.get("X-Idempotency-Key")

        ser = ForecastCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        dsv = DatasetVersion.objects.filter(
            tenant_id=tenant_id,
            dataset_version_id=data["datasetVersionId"]
        ).first()
        if not dsv:
            return Response({"detail": "DatasetVersion not found"}, status=404)
        if dsv.status != DatasetVersionStatus.READY:
            return Response({"detail": f"DatasetVersion not READY, status={dsv.status}"}, status=409)

        if idem_key:
            existing = ForecastJob.objects.filter(tenant_id=tenant_id, idempotency_key=idem_key).first()
            if existing:
                return Response({"forecastJobId": existing.forecast_job_id, "status": existing.status}, status=200)

        job = ForecastJob.objects.create(
            forecast_job_id=ForecastJob.new_job_id(),
            tenant_id=tenant_id,
            dataset_version=dsv,
            idempotency_key=idem_key,
            model_type=data["modelType"],
            params_json=data.get("params", {}),
            horizon=data["horizon"],
            status=JobStatus.PENDING,
        )
        return Response({"forecastJobId": job.forecast_job_id, "status": job.status}, status=201)

class ForecastDetailView(APIView):
    """
    GET /api/v1/forecasts/{jobId}/
    """
    def get(self, request, job_id: str):
        tenant_id = request.user.tenant_id
        job = ForecastJob.objects.filter(tenant_id=tenant_id, forecast_job_id=job_id).first()
        if not job:
            return Response({"detail": "Job not found"}, status=status.HTTP_404_NOT_FOUND)

        out = {
            "forecastJobId": job.forecast_job_id,
            "status": job.status,
            "modelType": job.model_type,
            "horizon": job.horizon,
            "createdAt": job.created_at.isoformat(),
            "startedAt": job.started_at.isoformat() if job.started_at else None,
            "finishedAt": job.finished_at.isoformat() if job.finished_at else None,
            "outputUri": job.output_uri,
            "errorMessage": job.error_message,
        }
        return Response(ForecastJobSerializer(out).data)

class ForecastResultView(APIView):
    """
    GET /api/v1/forecasts/{jobId}/result/
    """
    def get(self, request, job_id: str):
        tenant_id = request.user.tenant_id
        job = ForecastJob.objects.filter(tenant_id=tenant_id, forecast_job_id=job_id).first()
        if not job:
            return Response({"detail": "Job not found"}, status=status.HTTP_404_NOT_FOUND)

        if job.status != JobStatus.SUCCEEDED:
            return Response(
                {"detail": f"Job not ready, status={job.status}"},
                status=status.HTTP_409_CONFLICT
            )

        if not job.output_uri:
            return Response({"detail": "Missing outputUri"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        try:
            with open(job.output_uri, "r", encoding="utf-8") as f:
                payload = json.loads(f.read())
            return Response(ForecastResultSerializer(payload).data)
        except Exception as e:
            return Response({"detail": f"Failed to load artifact: {e}"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
