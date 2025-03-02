# Overview

This project was generated using the Astronomer CLI with the command `astro dev init`. It provides a pre-configured Airflow development environment that enables you to build, test, and deploy robust data pipelines quickly.

**ETL Overview:**  
- **Extraction & Ingestion:**  
  The ingestion DAG fetches historical weather data from the Open-Meteo Archive API and loads the raw JSON data into a PostgreSQL staging table. Since the data is static (i.e., it does not change over time for the given historical range), before inserting new data, any existing raw data is deleted. This avoids storage issues and prevents duplication while ensuring that the database always contains only the latest dataset.

- **Transformation:**  
  The transformation DAG retrieves the latest raw data from the staging table, transforms it by extracting hourly temperature records and attaching the ingestion timestamp, and then loads this structured data into a final PostgreSQL table. Before loading, any existing data in the final table is deleted so that only the fresh transformed data is stored, preventing unnecessary storage accumulation.

Together, these ETL pipelines efficiently process and manage historical weather data while optimizing database storage and avoiding redundant records.

# Project Contents

Your Astro project contains the following files and folders that support your Airflow development workflow:

- **dags/**  
  Contains the Python files for your Airflow DAGs.
  - **Custom Ingestion DAG – `historical_weather_ingestion`**  
    - Fetches historical weather data from the Open-Meteo Archive API.
    - Deletes any existing raw data from the PostgreSQL staging table and inserts the new raw JSON data with a fixed primary key (id=1).
    - Ensures that only one record exists to prevent unnecessary data accumulation.
    - Scheduled to run daily at 07:00 AM.
    
  - **Custom Transformation DAG – `historical_weather_transformation`**  
    - Retrieves the latest raw data along with its ingestion timestamp from the staging table.
    - Transforms the raw JSON data into structured hourly temperature records.
    - Deletes any existing records in the final table before inserting new transformed data.
    - Ensures that only the latest transformed data is stored, optimizing storage usage.
    - Triggered externally (typically by the ingestion DAG after successful data loading).
  
- **Dockerfile**  
  Defines the Astro Runtime Docker image used to run your Airflow instance, providing a controlled environment with all necessary dependencies.
  
- **include/**  
  Contains auxiliary files (e.g., SQL scripts, configuration files, static resources) required by your DAGs or project components.
  
- **packages.txt**  
  Lists any OS-level packages that your project requires; used during the Docker image build process.
  
- **requirements.txt**  
  Specifies the Python packages your project depends on; these are installed during Docker image build or deployment.
  
- **plugins/**  
  Contains custom or third-party Airflow plugins to extend Airflow’s capabilities.
  
- **airflow_settings.yaml**  
  Defines Airflow Connections, Variables, and Pools locally, streamlining configuration during development.

# Ingestion and Transformation Code

### Ingestion DAG – `historical_weather_ingestion`
- **Purpose:**  
  Fetches historical weather data from the Open-Meteo API and loads it into a PostgreSQL staging table.
  
- **Implementation Highlights:**  
  - **Data Extraction:**  
    Uses Airflow’s `HttpHook` to make a GET request to the Open-Meteo Archive API, retrieving JSON data for a specified latitude, longitude, and date range.
  - **Data Management:**  
    - Deletes any existing raw data from the staging table before inserting new data.
    - Inserts the new raw JSON data with a fixed primary key (id=1), ensuring that the table always contains the latest record.
  - **Avoiding Storage Issues:**  
    - Since the historical data remains unchanged over time, keeping multiple copies is unnecessary.
    - By maintaining only the latest record, storage usage is minimized, and duplication is prevented.
  - **Scheduling:**  
    Scheduled to run daily at 07:00 AM.

### Transformation DAG – `historical_weather_transformation`
- **Purpose:**  
  Transforms raw weather data from the staging table into a structured format and loads it into a final PostgreSQL table.
  
- **Implementation Highlights:**  
  - **Data Extraction:**  
    Retrieves the most recent raw record along with its ingestion timestamp from the staging table.
  - **Data Transformation:**  
    Parses the raw JSON data to extract hourly temperature records and attaches the ingestion timestamp for traceability.
  - **Data Management:**  
    - Deletes any existing records in the final table before inserting new transformed data.
    - Ensures that only the latest transformed data is stored to prevent unnecessary accumulation.
  - **Avoiding Storage Issues:**  
    - Since the historical data is static, transformed data does not need to be duplicated over multiple runs.
    - This deletion-before-insertion strategy prevents unnecessary growth of the table and optimizes database efficiency.
  - **Scheduling:**  
    Triggered externally (usually by the ingestion DAG) once data ingestion is complete.

# Deploy Your Project Locally

To begin developing and testing your DAGs locally, follow these steps:

1. **Start Airflow Locally:**  
   Run the command `astro dev start` from your project directory. This command starts your project in a local Docker environment, spinning up containers for:
   - **Postgres:** The Airflow metadata database.
   - **Webserver:** The component that renders the Airflow UI.
   - **Scheduler:** The component that monitors and triggers DAG tasks.
   - **Triggerer:** The component that triggers deferred tasks.

2. **Verify Running Containers:**  
   Use `docker ps` to ensure that all required containers have been created successfully.

3. **Access the Airflow UI:**  
   Open your web browser and navigate to [http://localhost:8080/](http://localhost:8080/). Log in with the default credentials (`admin`/`admin`). Here, you can view, trigger, and monitor your DAGs.

4. **Access the PostgreSQL Database:**  
   Your Airflow metadata is stored in a PostgreSQL database, accessible at `localhost:5432/postgres`.
