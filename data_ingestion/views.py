from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.validators import URLValidator
from django.core.exceptions import ValidationError
import json
import logging
import traceback
from .services import coordinate_argo_ingestion # Import the service function

logger = logging.getLogger(__name__)

# Mock Task (Same as before)
def argo_ingestion_task(argo_url):
    logger.info(f"Starting ingestion for URL: {argo_url}")
    # Call the service function synchronously (DANGEROUS IN PRODUCTION)
    total_saved = coordinate_argo_ingestion(argo_url)
    logger.info(f"Ingestion complete for {argo_url}. Saved {total_saved} records.")
    return total_saved

@csrf_exempt
def ingest_argo_data_handler(request):
    """
    Handles both GET (renders the form) and POST (handles API submission)
    on the same URL path.
    """
    if request.method == 'GET':
        # Handles the request to view the form
        # Assumes template is at 'argo_data/index.html'
        return render(request, 'url.html')

    elif request.method == 'POST':
        # Handles the API submission from the form's JavaScript
        try:
            # Check if request is JSON (as our frontend sends JSON)
            if request.content_type != 'application/json':
                 return JsonResponse({"error": "Content-Type must be application/json."}, status=415)
                 
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

            # --- Using synchronous execution for testing ONLY ---
            total_records_saved = argo_ingestion_task(argo_url)

            if total_records_saved > 0:
                return JsonResponse({
                    "message": "Data ingestion completed.",
                    "url_processed": argo_url,
                    "total_records_saved": total_records_saved,
                }, status=200)
            else:
                return JsonResponse({
                    "message": "Ingestion completed, but no new profile measurements were saved. Check server logs for details.",
                    "url_processed": argo_url,
                }, status=200)

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON format in request body."}, status=400)
        except Exception as e:
            logger.exception("Error during ARGO ingestion API call")
            return JsonResponse({"error": "An internal server error occurred during ingestion."}, status=500)

    else:
        # Should not happen if only GET and POST are allowed
        return JsonResponse({"error": "Method not allowed."}, status=405)
