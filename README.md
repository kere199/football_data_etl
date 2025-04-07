Premier League Football Data ETL and Dashboard Project
Overview
This project builds an end-to-end ETL pipeline to extract Premier League (PL) match data from the Football-Data.org API, process and store it in Google Cloud Platform (GCP), and visualize it in a Looker Studio dashboard. The pipeline fetches finished match data from the past 30 days, stores it in Google Cloud Storage (GCS), publishes a message to Google Cloud Pub/Sub, loads it into BigQuery, and provides advanced visualizations to analyze match outcomes, goals, and trends.

Project Components

Data Source: Football-Data.org API (Premier League matches).
ETL Pipeline:
  Extract: Flask app fetches data via API.
  Transform: Adds ingestion timestamp and processes JSON.
  Load: Stores in GCS, publishes to Pub/Sub, loads into BigQuery.
Storage:
  GCS: Raw JSON files.
  BigQuery: Structured tables (matches_raw and matches_production).
  Visualization: Looker Studio dashboard with advanced charts.
Prerequisites
  GCP Project: vital-cathode-454012-k0.
  API Key: Stored in Google Secret Manager as football-data-api-key.
  GCP Services: Cloud Run, GCS, Pub/Sub, BigQuery.
  Tools: Python 3.8+, Looker Studio account.


  ETL Process
1. Extract
Source: Football-Data.org API (http://api.football-data.org/v4/competitions/PL/matches).
Tool: Flask app deployed on Cloud Run.
Retrieve API key from Secret Manager.
Fetch finished PL matches from the last 30 days


Transform
Processing:
Add ingestion_timestamp to the JSON

Load
GCS Storage:
Store JSON in gs://football-data-lake-kere/matches/pl/<timestamp>.json:

Pub/Sub Notification:
Publish GCS path to football_topic


BigQuery Loading:
A separate process (not shown in app.py, assumed via Dataflow or manual load) moves data from GCS to BigQuery:
Raw Table: vital-cathode-454012-k0.football_dataset.matches_raw (stores raw JSON).
Production Table: vital-cathode-454012-k0.football_dataset.matches_production (flattened schema with fields like match_id, utc_date, score_fulltime_home, etc.).


Dashboard in Looker Studio
The dashboard visualizes Premier League match data with advanced charts, focusing on goals and outcomes.
