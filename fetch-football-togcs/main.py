import os
import json
import requests
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import secretmanager
from datetime import datetime, timedelta
from flask import Flask, request

app = Flask(__name__)

PROJECT_ID = "vital-cathode-454012-k0"
BUCKET_NAME = "football-data-lake-kere"
TOPIC_ID = "football_topic"  # Note: Updated to match your original topic name

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def access_secret(secret_id: str, project_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": secret_name})
    return response.payload.data.decode("UTF-8")

@app.route('/', methods=['POST'])
def fetch_football_to_gcs():
    api_key = access_secret("football-data-api-key", PROJECT_ID)
    headers = {"X-Auth-Token": api_key}

    today = datetime.utcnow()
    yesterday = today - timedelta(days=30)  # Last 30 days of data
    date_from = yesterday.strftime("%Y-%m-%d")
    date_to = today.strftime("%Y-%m-%d")

    # Updated URL to fetch all matches across all competitions
    url = f"https://api.football-data.org/v4/matches?status=FINISHED&dateFrom={date_from}&dateTo={date_to}"
    response = requests.get(url, headers=headers)
    match_data = response.json()

    ingestion_timestamp = datetime.utcnow().isoformat() + "Z"
    match_data["ingestion_timestamp"] = ingestion_timestamp

    # Store data with a generic path since it's not PL-specific anymore
    file_name = f"matches/all_competitions/{ingestion_timestamp.replace(':', '-')}.json"
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(match_data), content_type="application/json")

    # Publish to Pub/Sub
    message_data = json.dumps({"gcs_path": f"gs://{BUCKET_NAME}/{file_name}"}).encode("utf-8")
    print(f"Preparing to publish to {topic_path}: {message_data.decode('utf-8')}")
    future = publisher.publish(topic_path, message_data)
    message_id = future.result()
    print(f"Published message ID: {message_id}")

    return "Football data saved to GCS and published to Pub/Sub", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)