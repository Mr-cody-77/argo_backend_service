from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('RAG_communication.urls')),
    path('argo/', include('data_ingestion.urls')), 
    path('sql-query/', include('sql_query.urls')),
]
