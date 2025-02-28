from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json
import logging

# -----------------------------------------------------------------------------
# Constants and Configuration
# -----------------------------------------------------------------------------
# Coordinates for Koblenz, Germany
LATITUDE = '50.3536'
LONGITUDE = '7.5788'
# Date range for historical data extraction
START_DATE = '2025-01-01'
END_DATE = '2025-02-25'

# Airflow connection IDs for Postgres and the API endpoint
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# -----------------------------------------------------------------------------
# DAG Definition: historical_weather_ingestion
# Runs daily at 07:00 AM and extracts raw weather data from the Open-Meteo API.
# -----------------------------------------------------------------------------
with DAG(
    dag_id='historical_weather_ingestion',
    default_args=default_args,
    schedule_interval="0 7 * * *",  # Cron expression for 07:00 AM daily
    catchup=False
) as dag:

    @task()
    def extract_weather_data():
        """
        Extract historical weather data from the Open-Meteo Archive API.
        
        Uses the HttpHook to retrieve the JSON payload for a specified
        latitude, longitude, and date range. Raises an exception if the API
        call fails.
        """
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        # Construct the API endpoint URL with query parameters
        endpoint = (
            f"/v1/archive?latitude={LATITUDE}&longitude={LONGITUDE}"
            f"&start_date={START_DATE}&end_date={END_DATE}&hourly=temperature_2m"
        )
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def load_raw_weather_data(raw_data):
        """
        Load raw weather data into a PostgreSQL staging table if new data is available.
        
        This function checks if the new data (as JSON) differs from the latest
        stored record in the staging table. If the data is unchanged, the insertion
        is skipped to avoid duplicates.
        """
        # Initialize Postgres connection via PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create the staging table if it does not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_historical_weather_data (
                id SERIAL PRIMARY KEY,
                data JSONB,
                ingestion_time TIMESTAMP DEFAULT NOW()
            );
        """)

        # Serialize the new raw data to a JSON string with sorted keys
        new_data_json = json.dumps(raw_data, sort_keys=True)
        
        # Retrieve the most recent raw data record for comparison
        cursor.execute("""
            SELECT data FROM raw_historical_weather_data
            ORDER BY ingestion_time DESC
            LIMIT 1;
        """)
        row = cursor.fetchone()

        if row:
            # Serialize the existing record to ensure proper JSON comparison
            existing_data_json = json.dumps(row[0], sort_keys=True)
            if existing_data_json == new_data_json:
                logging.info("No new data available. Skipping insertion.")
                cursor.close()
                conn.close()
                return

        # Insert the new raw data since it differs from the latest record
        cursor.execute(
            "INSERT INTO raw_historical_weather_data (data) VALUES (%s)",
            (new_data_json,)
        )
        conn.commit()
        cursor.close()
        conn.close()

    # -----------------------------------------------------------------------------
    # DAG Workflow: Extract and then load raw weather data
    # -----------------------------------------------------------------------------
    raw_data = extract_weather_data()
    load_raw_weather_data(raw_data)
