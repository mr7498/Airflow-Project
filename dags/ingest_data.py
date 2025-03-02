from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import json
import logging

# -----------------------------------------------------------------------------
# Constants and Configuration
# -----------------------------------------------------------------------------
LATITUDE = '50.3536'
LONGITUDE = '7.5788'
START_DATE = '2025-01-01'
END_DATE = '2025-02-25'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# -----------------------------------------------------------------------------
# DAG Definition: historical_weather_ingestion
#
# This DAG extracts historical weather data from the Open-Meteo API,
# deletes any existing raw data from the PostgreSQL staging table,
# inserts the new data with primary key id=1 (ensuring a single refreshed row),
# and finally triggers the transformation DAG.
# -----------------------------------------------------------------------------
with DAG(
    dag_id='historical_weather_ingestion',
    default_args=default_args,
    schedule_interval="0 7 * * *",  # Runs daily at 07:00 AM
    catchup=False
) as dag:

    @task()
    def extract_weather_data():
        """
        Extract historical weather data from the Open-Meteo Archive API.
        
        Returns:
            dict: The JSON data returned by the API.
        """
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = (
            f"/v1/archive?latitude={LATITUDE}&longitude={LONGITUDE}"
            f"&start_date={START_DATE}&end_date={END_DATE}&hourly=temperature_2m"
        )
        response = http_hook.run(endpoint)
        if response.status_code == 200:
            logging.info("Successfully fetched weather data.")
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def delete_existing_data():
        """
        Delete any existing raw data from the PostgreSQL staging table.
        
        This ensures that only the latest data is present, and that the new
        record can be inserted with a primary key of 1.
        
        Returns:
            bool: True if deletion is successful.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Delete all existing records from the staging table.
        cursor.execute("DELETE FROM raw_historical_weather_data;")
        conn.commit()
        logging.info("Existing raw data deleted from staging table.")
        cursor.close()
        conn.close()
        return True

    @task()
    def load_raw_weather_data(raw_data):
        """
        Load raw weather data (stored as JSON) into the PostgreSQL staging table.
        The task inserts the new data with an explicit primary key of 1.
        
        Returns:
            bool: True if the new data was inserted successfully.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create the staging table if it does not exist.
        # The id column is defined as SERIAL PRIMARY KEY, but we will override it.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_historical_weather_data (
                id SERIAL PRIMARY KEY,
                data JSONB,
                ingestion_time TIMESTAMP DEFAULT NOW()
            );
        """)

        # Serialize the new raw data to a JSON string with sorted keys for consistency.
        new_data_json = json.dumps(raw_data, sort_keys=True)
        
        # Insert the new data with an explicit primary key value of 1.
        cursor.execute(
            "INSERT INTO raw_historical_weather_data (id, data) VALUES (%s, %s)",
            (1, new_data_json)
        )
        conn.commit()
        logging.info("New raw weather data inserted into staging table with id=1.")
        cursor.close()
        conn.close()
        return True

    # Trigger the transformation DAG after new data has been loaded.
    trigger_transformation = TriggerDagRunOperator(
        task_id="trigger_transformation_dag",
        trigger_dag_id="historical_weather_transformation",
        conf={"triggered_by": "historical_weather_ingestion"},
    )

    # ETL Workflow: Extract -> Delete Existing Data -> Load New Data -> Trigger Transformation DAG
    raw_data = extract_weather_data()
    deletion_status = delete_existing_data()
    data_loaded = load_raw_weather_data(raw_data)

    # Set task dependencies: deletion must complete before loading new data, then trigger transformation.
    deletion_status >> data_loaded >> trigger_transformation