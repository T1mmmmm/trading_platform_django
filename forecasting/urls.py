from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import SignalRunStartView, SignalRunDetailView, SignalRunResultView, TradeSimRunCreateView, TradeSimRunDetailView, TradeSimRunResultView, DatasetCreateView, DatasetCommitView, DatasetVersionDetailView, DatasetUploadView, HealthView, ForecastListCreateView, ForecastDetailView, ForecastResultView, SimAccountViewSet, StrategyViewSet, BacktestCreateView, BacktestDetailView, BacktestResultView, ReportCreateView, ReportDetailView


router = DefaultRouter()
router.register(r'strategies', StrategyViewSet, basename='strategy')
router.register(r'sim/accounts', SimAccountViewSet, basename='sim_account')


urlpatterns = [
    path("signals:run", SignalRunStartView.as_view()),
    path("signals/runs/<str:signal_run_id>", SignalRunDetailView.as_view()),
    path("signals/runs/<str:signal_run_id>/result", SignalRunResultView.as_view()),
    path("sim/runs", TradeSimRunCreateView.as_view()),
    path("sim/runs/<str:trade_sim_run_id>", TradeSimRunDetailView.as_view()),
    path("sim/runs/<str:trade_sim_run_id>/result", TradeSimRunResultView.as_view()),
    path("health/", HealthView.as_view()),
    path("datasets/", DatasetCreateView.as_view()),
    path("datasets/<str:dataset_id>/versions:commit", DatasetCommitView.as_view()),
    path("datasets/<str:dataset_id>/versions:upload", DatasetUploadView.as_view()),
    path("datasets/<str:dataset_id>/versions/<str:dataset_version_id>/", DatasetVersionDetailView.as_view()),

    path("forecasts/", ForecastListCreateView.as_view()),
    path("forecasts/<str:job_id>/", ForecastDetailView.as_view()),
    path("forecasts/<str:job_id>/result/", ForecastResultView.as_view()),
    path('', include(router.urls)),

    path("backtests/", BacktestCreateView.as_view()),
    path("backtests/<str:backtest_run_id>/", BacktestDetailView.as_view()),
    path("backtests/<str:backtest_run_id>/result", BacktestResultView.as_view()),
    path("reports/", ReportCreateView.as_view()),
    path("reports/<str:report_id>/", ReportDetailView.as_view()),
]
