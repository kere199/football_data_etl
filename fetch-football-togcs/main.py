import os
import json
import requests
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import secretmanager
from datetime import datetime, timedelta

PROJECT_ID = "vital-cathode-454012-k0"
BUCKET_NAME = "football-data-lake-kere"
TOPIC_ID = "football_topic"

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def access_secret(secret_id: str, project_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": secret_name})
    return response.payload.data.decode("UTF-8")

def fetch_football_to_gcs(event, context):
    api_key = access_secret("b0e602a67fbb4441ab2127b5fda43223", PROJECT_ID)
    headers = {"X-Auth-Token": api_key}

    today = datetime.utcnow()
    yesterday = today - timedelta(days=1)
    date_from = yesterday.strftime("%Y-%m-%d")
    date_to = today.strftime("%Y-%m-%d")

    url = f"http://api.football-data.org/v4/competitions/PL/matches?status=FINISHED&dateFrom={date_from}&dateTo={date_to}"
    response = requests.get(url, headers=headers)
    match_data = response.json()

    ingestion_timestamp = context.timestamp
    match_data["ingestion_timestamp"] = ingestion_timestamp

    file_name = f"matches/pl/{ingestion_timestamp.replace(':', '-')}.json"
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(match_data), content_type="application/json")

    message_data = json.dumps({"gcs_path": f"gs://{BUCKET_NAME}/{file_name}"}).encode("utf-8")
    future = publisher.publish(topic_path, message_data)
    future.result()

    return "Football data saved to GCS and published to Pub/Sub", 200