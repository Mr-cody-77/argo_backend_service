from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.db.models import Avg
import json
import logging
from datetime import datetime
from django.shortcuts import render

from data_ingestion.models import ArgoProfileData, ArgoMeasurement

logger = logging.getLogger(__name__)


@csrf_exempt
def sql_query_argo_data(request):
    """
    API endpoint to query floats with filters:
    min_lat, max_lat, ocean_name, start_date, end_date.
    Works with both GET query params and POST JSON.
    """
    try:
        # --- Parse inputs depending on request method ---
        if request.method == "POST":
            try:
                data = json.loads(request.body)
            except json.JSONDecodeError:
                return JsonResponse({"error": "Invalid JSON body"}, status=400)
        else:  # GET fallback
            data = request.GET.dict()

        # Extract filters with defaults
        min_lat = float(data.get("min_lat", -90))
        max_lat = float(data.get("max_lat", 90))
        ocean_name = data.get("ocean_name") or None
        start_date = data.get("start_date") or None
        end_date = data.get("end_date") or None

        # --- Base Profile Query ---
        profiles = ArgoProfileData.objects.filter(
            latitude__gte=min_lat,
            latitude__lte=max_lat,
        )

        if start_date:
            try:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
                profiles = profiles.filter(juld_date__gte=start_date)
            except ValueError:
                return JsonResponse({"error": "Invalid start_date format, expected YYYY-MM-DD"}, status=400)

        if end_date:
            try:
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
                profiles = profiles.filter(juld_date__lte=end_date)
            except ValueError:
                return JsonResponse({"error": "Invalid end_date format, expected YYYY-MM-DD"}, status=400)

        if ocean_name:
            profiles = profiles.filter(ocean_name__iexact=ocean_name)

        # --- Join with Measurements + Aggregate ---
        results = (
            ArgoMeasurement.objects.filter(profile__in=profiles)
            .values(
                "profile__platform_number",
                "profile__cycle_number",
                "profile__juld_date",
                "profile__latitude",
                "profile__longitude",
                "profile__ocean_name",
            )
            .annotate(
                avg_temp=Avg("temperature"),
                avg_sal=Avg("salinity"),
                avg_pres=Avg("pressure"),
            )
            .order_by("profile__platform_number", "profile__cycle_number")
        )

        # --- Format Response ---
        formatted = [
            {
                "platform_number": r["profile__platform_number"],
                "cycle_number": r["profile__cycle_number"],
                "date": r["profile__juld_date"].strftime("%Y-%m-%d %H:%M:%S")
                if r["profile__juld_date"] else None,
                "latitude": r["profile__latitude"],
                "longitude": r["profile__longitude"],
                "ocean_name": r["profile__ocean_name"],
                "temperature_mean": round(r["avg_temp"], 3) if r["avg_temp"] is not None else None,
                "salinity_mean": round(r["avg_sal"], 3) if r["avg_sal"] is not None else None,
                "pressure_mean": round(r["avg_pres"], 3) if r["avg_pres"] is not None else None,
            }
            for r in results
        ]

        return JsonResponse({"count": len(formatted), "results": formatted}, status=200)

    except Exception as e:
        logger.exception("Error while querying ARGO data")
        return JsonResponse({"error": str(e)}, status=500)
