import os
from forecasting.celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")
app = Celery("tp")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()