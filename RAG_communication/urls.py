from django.urls import path
from .views import query_rag

urlpatterns = [
    path("ask/", query_rag, name="query_rag"),
]
