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

from .models import Dataset, DatasetVersion, DatasetVersionStatus, ForecastJob, JobStatus
from .serializers import (
    DatasetCreateSerializer, DatasetCreateResponseSerializer,
    DatasetCommitSerializer, DatasetCommitResponseSerializer,
    DatasetUploadSerializer, DatasetUploadResponseSerializer,
    DatasetVersionSerializer,
    ForecastCreateSerializer,
    ForecastCreateResponseSerializer,
    ForecastJobSerializer,
    ForecastResultSerializer,
)

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
