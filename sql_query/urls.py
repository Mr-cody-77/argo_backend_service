from django.urls import path
from .views import sql_query_argo_data

urlpatterns = [
    path('lookup-table/', sql_query_argo_data, name='sql_lookup_table'),
]