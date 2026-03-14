from django.urls import path

from .views import LLMTaskDetailView, LLMTaskListCreateView, LLMTaskResultView


urlpatterns = [
    path("tasks", LLMTaskListCreateView.as_view()),
    path("tasks/<str:llm_task_id>", LLMTaskDetailView.as_view()),
    path("tasks/<str:llm_task_id>/result", LLMTaskResultView.as_view()),
]
