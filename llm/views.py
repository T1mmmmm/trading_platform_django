from pathlib import Path

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from forecasting.models import BacktestRun, BacktestStatus
from .models import LLMTask, Report, JobStatus
from .serializers import (
    LLMTaskCreateSerializer,
    LLMTaskCreateResponseSerializer,
)


class LLMTaskListCreateView(APIView):
    def post(self, request):
        tenant_id = request.user.tenant_id

        ser = LLMTaskCreateSerializer(data=request.data)
        ser.is_valid(raise_exception=True)
        data = ser.validated_data

        source_type = data["sourceType"]
        source_id = data["sourceId"]

        if source_type != "BACKTEST":
            return Response(
                {"detail": "Only sourceType=BACKTEST is supported"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        backtest = BacktestRun.objects.filter(
            tenant_id=tenant_id,
            backtest_run_id=source_id,
        ).first()
        if not backtest:
            return Response(
                {"detail": "BacktestRun not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        if backtest.status not in {BacktestStatus.METRICS_DONE, BacktestStatus.REPORT_DONE}:
            return Response(
                {"detail": f"BacktestRun not ready, status={backtest.status}"},
                status=status.HTTP_409_CONFLICT,
            )

        task = LLMTask.objects.create(
            llm_task_id=LLMTask.new_task_id(),
            tenant_id=tenant_id,
            task_type=data["taskType"],
            source_type=source_type,
            source_id=source_id,
            prompt_template_version=data.get("promptTemplateVersion", "v1"),
            status=JobStatus.PENDING,
            input_refs_json={
                "backtestRunId": backtest.backtest_run_id,
                "datasetVersionId": backtest.dataset_version.dataset_version_id,
                "strategyId": backtest.strategy.strategy_id,
            },
        )

        return Response(
            LLMTaskCreateResponseSerializer(
                {"llmTaskId": task.llm_task_id, "status": task.status}
            ).data,
            status=status.HTTP_201_CREATED,
        )


class LLMTaskDetailView(APIView):
    def get(self, request, llm_task_id: str):
        tenant_id = request.user.tenant_id
        task = LLMTask.objects.filter(tenant_id=tenant_id, llm_task_id=llm_task_id).first()
        if not task:
            return Response({"detail": "Task not found"}, status=status.HTTP_404_NOT_FOUND)

        return Response({
            "llmTaskId": task.llm_task_id,
            "taskType": task.task_type,
            "sourceType": task.source_type,
            "sourceId": task.source_id,
            "status": task.status,
            "modelName": task.model_name,
            "outputUri": task.output_uri,
            "errorMessage": task.error_message,
            "createdAt": task.created_at,
            "startedAt": task.started_at,
            "finishedAt": task.finished_at,
        })


class LLMTaskResultView(APIView):
    def get(self, request, llm_task_id: str):
        tenant_id = request.user.tenant_id
        task = LLMTask.objects.filter(tenant_id=tenant_id, llm_task_id=llm_task_id).first()
        if not task:
            return Response({"detail": "Task not found"}, status=status.HTTP_404_NOT_FOUND)

        if task.status != JobStatus.SUCCEEDED:
            return Response({"detail": f"Task not ready, status={task.status}"}, status=status.HTTP_409_CONFLICT)

        if not task.output_uri:
            return Response({"detail": "Missing outputUri"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        content = Path(task.output_uri).read_text(encoding="utf-8")

        report = Report.objects.filter(tenant_id=tenant_id, llm_task_id=task.llm_task_id).first()

        return Response({
            "llmTaskId": task.llm_task_id,
            "reportId": report.report_id if report else None,
            "format": "MARKDOWN",
            "content": content,
        })
