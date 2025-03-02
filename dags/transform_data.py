from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

# -----------------------------------------------------------------------------
# Constants and Configuration
# -----------------------------------------------------------------------------
LATITUDE = '50.3536'
LONGITUDE = '7.5788'
POSTGRES_CONN_ID = 'postgres_default'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# -----------------------------------------------------------------------------
# DAG Definition: historical_weather_transformation
#
# This DAG transforms raw weather data stored in PostgreSQL into a structured
# format and then loads it into a final table. Before inserting new data, it 
# deletes any existing records from the final table.
#
# The DAG is intended to be triggered externally (e.g., via TriggerDagRunOperator)
# and starts with a short delay to allow finalization steps from the ingestion DAG.
# -----------------------------------------------------------------------------
with DAG(
    dag_id='historical_weather_transformation',
    default_args=default_args,
    schedule_interval=None,  # This DAG is triggered externally.
    catchup=False
) as dag:

    # Sleep for 5 seconds to allow any finalization steps from the ingestion DAG.
    wait_for_start = BashOperator(
        task_id="wait_for_start",
        bash_command="sleep 5"
    )

    @task()
    def extract_raw_data():
        """
        Extract the latest raw weather data and its ingestion_time from PostgreSQL.
        
        Returns:
            dict: A dictionary containing the raw JSON data and the ingestion timestamp.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        # Retrieve the latest raw data record and its ingestion time.
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
            # row[0] contains the JSONB data; row[1] contains the ingestion timestamp.
            return {"data": row[0], "ingestion_time": row[1]}
        else:
            raise Exception("No raw weather data found.")

    @task()
    def transform_weather_data(raw_record):
        """
        Transform raw JSON weather data into structured records and attach the ingestion_time.
        
        Args:
            raw_record (dict): Contains raw JSON data and its ingestion timestamp.
            
        Returns:
            list: A list of dictionaries, each representing a structured weather record.
        """
        raw_data = raw_record["data"]
        ingestion_time = raw_record["ingestion_time"]
        # Assume the raw data contains an 'hourly' key with time and temperature arrays.
        hourly_data = raw_data.get('hourly', {})
        timestamps = hourly_data.get('time', [])
        temperatures = hourly_data.get('temperature_2m', [])
        
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
    def delete_existing_transformed_data():
        """
        Delete all existing records from the final PostgreSQL table.
        
        This ensures that only the latest transformed data is present in the table.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        # Delete all records from the final table.
        cursor.execute("DELETE FROM historical_weather_data;")
        conn.commit()
        cursor.close()
        conn.close()
        return "Existing data deleted."

    @task()
    def load_transformed_data(transformed_data):
        """
        Load the transformed weather data into the final PostgreSQL table.
        
        Assumes that the final table has been cleared of existing data.
        Returns:
            str: A message indicating successful data insertion.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create the final table if it does not exist.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS historical_weather_data (
                latitude FLOAT,
                longitude FLOAT,
                timestamp TIMESTAMP,
                temperature FLOAT,
                raw_ingestion_time TIMESTAMP
            );
        """)

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

    # Define the ETL workflow sequence:
    # 1. Wait for finalization from ingestion DAG.
    # 2. Extract the latest raw data.
    # 3. Transform the raw data.
    # 4. Delete any existing transformed data.
    # 5. Load the new transformed data.
    raw_record = extract_raw_data()
    structured_data = transform_weather_data(raw_record)
    deletion_status = delete_existing_transformed_data()
    data_loaded = load_transformed_data(structured_data)

    # Set task dependencies: wait -> extract -> transform -> delete -> load.
    wait_for_start >> raw_record
    raw_record >> structured_data
    structured_data >> deletion_status >> data_loaded