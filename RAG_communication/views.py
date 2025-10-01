import requests
import logging
import os
import json
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

logger = logging.getLogger(__name__)

# URL of your Flask RAG model deployed on Render
URL = os.getenv("FLASK_URL", "http://127.0.0.1:8000/ask")
# URL="http://127.0.0.1:5000/ask"

@csrf_exempt
def query_rag(request):
    response_data = None
    error_message = None

    if request.method == "POST":
        try:
            # Parse JSON body instead of request.POST
            data = json.loads(request.body)
            user_query = data.get("query", "")
        except Exception as e:
            return JsonResponse({"error": f"Invalid JSON body: {str(e)}"}, status=400)

        try:
            # Increased timeout to handle slow API
            resp = requests.post(URL, json={"query": user_query}, timeout=30)

            # Log raw response for debugging
            logger.debug(f"Flask API raw response: {resp.text}")

            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if "answer" in data:
                        response_data = data["answer"]
                    else:
                        error_message = f"Flask API returned JSON but no 'answer' field: {data}"
                        logger.error(error_message)
                except ValueError:
                    error_message = f"Flask API returned invalid JSON: {resp.text}"
                    logger.error(error_message)
            else:
                error_message = f"Flask API returned status {resp.status_code}: {resp.text}"
                logger.error(error_message)

        except requests.exceptions.Timeout:
            error_message = (
                "Request to the RAG API timed out after 30 seconds. "
                "The server may be slow to start or heavily loaded."
            )
            logger.error(error_message)
        except requests.exceptions.RequestException as e:
            error_message = f"Request to Flask API failed: {str(e)}"
            logger.error(error_message)

        # Return JSON response for React frontend
        return JsonResponse({
            "answer": response_data or "",      # RAG answer
            "sql_query": "",                    # Optional: SQL query if available
            "sql_rows": [],                     # Optional: SQL result rows
            "error": error_message
        })

    # Method not allowed
    return JsonResponse({"error": "Invalid request method"}, status=405)
