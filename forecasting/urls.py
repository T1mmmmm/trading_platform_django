from django.urls import path
from .views import DatasetCreateView, DatasetCommitView, DatasetVersionDetailView, DatasetUploadView, HealthView, ForecastListCreateView, ForecastDetailView, ForecastResultView

urlpatterns = [
    path("health/", HealthView.as_view()),
    path("datasets/", DatasetCreateView.as_view()),
    path("datasets/<str:dataset_id>/versions:commit", DatasetCommitView.as_view()),
    path("datasets/<str:dataset_id>/versions:upload", DatasetUploadView.as_view()),
    path("datasets/<str:dataset_id>/versions/<str:dataset_version_id>/", DatasetVersionDetailView.as_view()),

    path("forecasts/", ForecastListCreateView.as_view()),
    path("forecasts/<str:job_id>/", ForecastDetailView.as_view()),
    path("forecasts/<str:job_id>/result/", ForecastResultView.as_view()),
]
