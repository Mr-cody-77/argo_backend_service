import requests
from django.shortcuts import render
import logging

logger = logging.getLogger(__name__)

# URL of your Flask RAG model deployed on Render
URL = "https://sih-25.onrender.com/ask"

def query_rag(request):
    response_data = None
    error_message = None

    if request.method == "POST":
        user_query = request.POST.get("query")
        try:
            # 🚀 FIX: Increased timeout from 10 seconds to 30 seconds
            # This compensates for cold start delays common on cloud hosts like Render.
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
                    # JSON decoding failed
                    error_message = f"Flask API returned invalid JSON: {resp.text}"
                    logger.error(error_message)
            else:
                # Non-200 HTTP code from Flask
                error_message = f"Flask API returned status {resp.status_code}: {resp.text}"
                logger.error(error_message)

        except requests.exceptions.Timeout:
            # Specific handling for the timeout error
            error_message = "Request to the RAG API timed out after 30 seconds. The server may be slow to start or heavily loaded."
            logger.error(error_message)

        except requests.exceptions.RequestException as e:
            # Other connection errors
            error_message = f"Request to Flask API failed: {str(e)}"
            logger.error(error_message)

    return render(request, "query_rag.html", {
        "response": response_data,
        "error": error_message
    })
