import os
import json
import base64
from google.cloud import storage
from google.cloud import bigquery
from flask import Flask, request

app = Flask(__name__)

PROJECT_ID = "vital-cathode-454012-k0"
DATASET_ID = "football_dataset"
TABLE_ID = "matches_raw"  # Changed to matches_raw
BUCKET_NAME = "football-data-lake-kere"

storage_client = storage.Client()
bq_client = bigquery.Client()

@app.route('/', methods=['POST'])
def gcs_to_bigquery():
    try:
        # Parse Pub/Sub message
        envelope = request.get_json()
        if not envelope or 'data' not in envelope.get('message', {}):
            print("Invalid Pub/Sub message format")
            return "Invalid Pub/Sub message", 400

        message_data = base64.b64decode(envelope['message']['data']).decode("utf-8")
        message = json.loads(message_data)
        gcs_path = message.get("gcs_path")

        if not gcs_path or not gcs_path.startswith(f"gs://{BUCKET_NAME}/"):
            print(f"Invalid GCS path: {gcs_path}")
            return "Invalid GCS path", 400

        # Read from GCS
        bucket_name = gcs_path.split("/")[2]
        blob_name = "/".join(gcs_path.split("/")[3:])
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        match_data = json.loads(blob.download_as_string())

        # Extract matches
        matches = match_data.get("matches", [])
        if not matches:
            print("No matches found in GCS file")
            return "No matches to process", 200

        # Prepare rows for BigQuery
        rows = [
            {
                "match_id": match["id"],
                "utc_date": match["utcDate"],
                "status": match["status"],
                "matchday": match["matchday"],
                "stage": match["stage"],
                "home_team_id": match["homeTeam"]["id"],
                "home_team_name": match["homeTeam"]["name"],
                "away_team_id": match["awayTeam"]["id"],
                "away_team_name": match["awayTeam"]["name"],
                "score_fulltime_home": match["score"]["fullTime"]["home"] if match["score"]["fullTime"]["home"] is not None else 0,
                "score_fulltime_away": match["score"]["fullTime"]["away"] if match["score"]["fullTime"]["away"] is not None else 0,
                "score_halftime_home": match["score"]["halfTime"]["home"] if match["score"]["halfTime"]["home"] is not None else 0,
                "score_halftime_away": match["score"]["halfTime"]["away"] if match["score"]["halfTime"]["away"] is not None else 0,
                "winner": match["score"]["winner"],
                "last_updated": match["lastUpdated"],
                "ingestion_timestamp": match_data["ingestion_timestamp"]
            }
            for match in matches
        ]

        # Load into BigQuery (append only)
        table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
        errors = bq_client.insert_rows_json(table_ref, rows)
        if errors:
            print(f"Errors inserting rows: {errors}")
            return f"Failed to load data into BigQuery: {errors}", 500

        print(f"Successfully loaded {len(rows)} rows into BigQuery")
        return "Data loaded into BigQuery", 200
    except Exception as e:
        print(f"Error processing request: {e}")
        return f"Error: {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)