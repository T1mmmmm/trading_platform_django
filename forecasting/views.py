from django.shortcuts import render

# Create your views here.
import json
from django.utils import timezone
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .models import ForecastJob, JobStatus
from .serializers import (
    ForecastCreateSerializer,
    ForecastCreateResponseSerializer,
    ForecastJobSerializer,
    ForecastResultSerializer,
)

class HealthView(APIView):
    authentication_classes = []
    permission_classes = []

    def get(self, request):
        return Response({"status": "ok"})

class ForecastListCreateView(APIView):
    """
    POST /api/v1/forecasts/
    """
    def post(self, request):
        tenant_id = request.user.tenant_id
        idem_key = request.headers.get("X-Idempotency-Key")

        ser = ForecastCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        # 幂等：同 tenant + idemKey 返回同一个 job
        if idem_key:
            existing = ForecastJob.objects.filter(
                tenant_id=tenant_id, idempotency_key=idem_key
            ).first()
            if existing:
                out = {"forecastJobId": existing.forecast_job_id, "status": existing.status}
                return Response(ForecastCreateResponseSerializer(out).data)

        job = ForecastJob.objects.create(
            forecast_job_id=ForecastJob.new_job_id(),
            tenant_id=tenant_id,
            idempotency_key=idem_key,
            model_type=data["modelType"],
            params_json=data.get("params", {}),
            horizon=data["horizon"],
            status=JobStatus.PENDING,
        )

        out = {"forecastJobId": job.forecast_job_id, "status": job.status}
        return Response(ForecastCreateResponseSerializer(out).data, status=status.HTTP_201_CREATED)

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
