from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError
import json
import logging
import traceback
# Imports the functions that now handle geolocation and DB storage
from .services import coordinate_argo_ingestion, process_uploaded_netcdf_file 

logger = logging.getLogger(__name__)

# Mock Task (for URL ingestion)
def argo_ingestion_task(argo_url):
    """Delegates the ingestion task for a URL to the service layer."""
    logger.info(f"Starting ingestion for URL: {argo_url}")
    # The coordinate_argo_ingestion function now handles calling get_nearest_ocean
    total_saved = coordinate_argo_ingestion(argo_url)
    logger.info(f"Ingestion complete for {argo_url}. Saved {total_saved} records.")
    return total_saved


@csrf_exempt
def ingest_argo_data_handler(request):
    """
    Handles both GET (renders the form) and POST (handles API submission).
    Supports:
      - JSON POST with 'argo_url' → download from URL & ingest
      - multipart/form-data POST with 'file' → ingest uploaded .nc file
    """
    if request.method == 'GET':
        # Assumes a template named 'url.html' exists in myapp/templates/
        return render(request, 'url.html')

    elif request.method == 'POST':
        try:
            # --- Case 1: File Upload (multipart/form-data) ---
            if request.content_type.startswith("multipart/form-data"):
                if "file" not in request.FILES:
                    return JsonResponse({"error": "No file provided in request."}, status=400)

                uploaded_file = request.FILES["file"]
                logger.info(f"📂 Received file upload: {uploaded_file.name} ({uploaded_file.size} bytes)")

                # Delegation to service function (which handles ocean name calculation)
                total_records_saved = process_uploaded_netcdf_file(uploaded_file)

                return JsonResponse({
                    "message": "File ingestion completed. Ocean names have been stored.",
                    "filename": uploaded_file.name,
                    "total_records_saved": total_records_saved,
                }, status=200)

            # --- Case 2: URL ingestion (JSON) ---
            elif request.content_type == 'application/json':
                data = json.loads(request.body)
                argo_url = data.get('argo_url', '').strip()

                if not argo_url:
                    return JsonResponse({"error": "The 'argo_url' field is required."}, status=400)

                # Validate URL format
                val = URLValidator()
                try:
                    val(argo_url)
                except ValidationError:
                    return JsonResponse({"error": "The provided URL is not valid."}, status=400)

                # Delegation to service function (which handles ocean name calculation)
                total_records_saved = argo_ingestion_task(argo_url)

                return JsonResponse({
                    "message": "Data ingestion completed. Ocean names have been stored.",
                    "url_processed": argo_url,
                    "total_records_saved": total_records_saved,
                }, status=200)

            else:
                return JsonResponse({"error": "Unsupported Content-Type. Use JSON or multipart/form-data."}, status=415)

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON format in request body."}, status=400)
        except Exception as e:
            logger.exception("Error during ARGO ingestion API call")
            return JsonResponse({"error": "An internal server error occurred during ingestion."}, status=500)

    else:
        return JsonResponse({"error": "Method not allowed."}, status=405)
