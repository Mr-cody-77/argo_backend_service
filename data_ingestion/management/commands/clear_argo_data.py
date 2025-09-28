from django.core.management.base import BaseCommand
from data_ingestion.models import ArgoProfile, ArgoMeasurement

class Command(BaseCommand):
    help = 'Delete all Argo data'

    def handle(self, *args, **kwargs):
        ArgoMeasurement.objects.all().delete()
        ArgoProfile.objects.all().delete()
        self.stdout.write(self.style.SUCCESS('✅ All Argo data deleted!'))