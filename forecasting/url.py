from django.urls import path
from .views import HealthView, ForecastListCreateView, ForecastDetailView, ForecastResultView

urlpatterns = [
    path("health/", HealthView.as_view()),

    path("forecasts/", ForecastListCreateView.as_view()),
    path("forecasts/<str:job_id>/", ForecastDetailView.as_view()),
    path("forecasts/<str:job_id>/result/", ForecastResultView.as_view()),
]
