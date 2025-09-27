# In argo_data/urls.py

from django.urls import path
from .views import ingest_argo_data_handler

urlpatterns = [
    path('ingest-url/', ingest_argo_data_handler, name='argo_ingestion_page'),
]