from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

# -----------------------------------------------------------------------------
# Constants and Configuration
# -----------------------------------------------------------------------------
# Coordinates for Koblenz, Germany
LATITUDE = '50.3536'
LONGITUDE = '7.5788'

# Airflow connection ID for PostgreSQL
POSTGRES_CONN_ID = 'postgres_default'

# Default arguments for the DAG execution
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# -----------------------------------------------------------------------------
# DAG Definition: historical_weather_transformation
# Runs daily at 07:30 AM to transform raw weather data into a structured format.
# -----------------------------------------------------------------------------
with DAG(
    dag_id='historical_weather_transformation',
    default_args=default_args,
    schedule_interval='30 7 * * *',  # Cron expression for 07:30 AM daily
    catchup=False
) as dag:

    @task()
    def extract_raw_data():
        """
        Extract the latest raw weather data and its ingestion_time from PostgreSQL.

        Retrieves the most recent record from the staging table 'raw_historical_weather_data'.
        Returns:
            dict: A dictionary containing the raw JSON data and its ingestion_time.
        Raises:
            Exception: If no raw weather data is found.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Retrieve the most recent raw data along with its ingestion_time
        cursor.execute("""
            SELECT data, ingestion_time
            FROM raw_historical_weather_data 
            ORDER BY ingestion_time DESC 
            LIMIT 1;
        """)
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            # row[0] contains JSONB data; row[1] is the ingestion_time timestamp
            return {"data": row[0], "ingestion_time": row[1]}
        else:
            raise Exception("No raw weather data found.")

    @task()
    def transform_weather_data(raw_record):
        """
        Transform raw JSON weather data into structured records.

        This function extracts hourly weather information from the raw JSON data,
        and attaches the raw ingestion timestamp to each record.
        Args:
            raw_record (dict): A dictionary containing 'data' (raw JSON) and 'ingestion_time'.
        Returns:
            list: A list of dictionaries, each representing a structured weather record.
        """
        raw_data = raw_record["data"]
        ingestion_time = raw_record["ingestion_time"]

        # Extract the 'hourly' data section, which should include time and temperature arrays.
        hourly_data = raw_data.get('hourly', {})
        timestamps = hourly_data.get('time', [])
        temperatures = hourly_data.get('temperature_2m', [])
        
        # Build a list of structured records including the ingestion timestamp for traceability.
        transformed_data = [
            {
                'latitude': LATITUDE,
                'longitude': LONGITUDE,
                'timestamp': timestamps[i],
                'temperature': temperatures[i],
                'raw_ingestion_time': ingestion_time
            }
            for i in range(len(timestamps))
        ]
        return transformed_data

    @task()
    def load_transformed_data(transformed_data):
        """
        Load transformed weather data into the final PostgreSQL table if not already loaded.

        This function first creates the final table if it does not exist. It then checks if
        the data corresponding to the current raw ingestion timestamp already exists to avoid duplication.
        Args:
            transformed_data (list): A list of structured weather records.
        Returns:
            str: A message indicating whether data was loaded or skipped.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create the final table for transformed data, including a column for the raw ingestion timestamp.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_weather_data (
                latitude FLOAT,
                longitude FLOAT,
                timestamp TIMESTAMP,
                temperature FLOAT,
                raw_ingestion_time TIMESTAMP
            );
        """)

        # Use the raw_ingestion_time from the first record to check for duplicates.
        raw_ingestion_time = transformed_data[0]['raw_ingestion_time']
        cursor.execute("""
            SELECT 1 FROM historical_weather_data
            WHERE raw_ingestion_time = %s
            LIMIT 1;
        """, (raw_ingestion_time,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            return "No new data to load."

        # Insert each transformed record into the final table.
        insert_query = """
            INSERT INTO historical_weather_data (latitude, longitude, timestamp, temperature, raw_ingestion_time)
            VALUES (%s, %s, %s, %s, %s)
        """
        for record in transformed_data:
            cursor.execute(insert_query, (
                record['latitude'],
                record['longitude'],
                record['timestamp'],
                record['temperature'],
                record['raw_ingestion_time']
            ))
        conn.commit()
        cursor.close()
        conn.close()
        return "Data loaded successfully."

    # -----------------------------------------------------------------------------
    # Define the ETL Workflow:
    # 1. Extract the latest raw data record from PostgreSQL.
    # 2. Transform the raw data into structured format.
    # 3. Load the transformed data into the final table if not already loaded.
    # -----------------------------------------------------------------------------
    raw_record = extract_raw_data()
    structured_data = transform_weather_data(raw_record)
    load_transformed_data(structured_data)