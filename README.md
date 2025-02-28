# Overview

This project was generated using the Astronomer CLI with the command `astro dev init`. It provides a pre-configured Airflow development environment that enables you to build, test, and deploy robust data pipelines quickly.

**ETL Overview:**  
- **Extraction:**  
  The ingestion DAG fetches historical weather data from the Open-Meteo Archive API and loads the raw JSON data into a PostgreSQL staging table. It ensures that only new data is stored by checking for duplicates.
  
- **Transformation:**  
  The transformation DAG retrieves the latest raw data, parses it to extract hourly temperature records, and attaches an ingestion timestamp for traceability. It then loads this structured data into a final PostgreSQL table, ensuring that duplicate transformed entries are avoided.
  
Together, these ETL pipelines enable you to efficiently process and analyze historical weather data in a reliable, production-ready Airflow environment.

# Project Contents

Your Astro project contains the following files and folders that support your Airflow development workflow:

- **dags/**  
  This folder contains the Python files for your Airflow DAGs.
  - **Custom Ingestion DAG – `historical_weather_ingestion`**  
    This DAG extracts historical weather data from the Open-Meteo Archive API and loads the raw JSON data into a PostgreSQL staging table. It is scheduled to run daily at 07:00 AM and includes logic to check for duplicate data before insertion.
    
    **Key features:**
    - Uses Airflow’s `HttpHook` to call the API.
    - Converts API responses to JSON and stores them in the `raw_historical_weather_data` table.
    - Prevents duplicate entries by comparing new API data with the most recent stored record.
  
  - **Custom Transformation DAG – `historical_weather_transformation`**  
    This DAG retrieves the latest raw weather data from the staging table, transforms it into a structured format, and loads it into a final PostgreSQL table. It is scheduled to run daily at 07:30 AM.
    
    **Key features:**
    - Extracts raw data along with its ingestion timestamp from PostgreSQL.
    - Processes the raw JSON data to extract hourly temperature data.
    - Attaches the ingestion timestamp for traceability.
    - Checks for duplicate transformed data before loading it into the `historical_weather_data` table.

- **Dockerfile**  
  This file defines the Astro Runtime Docker image used to run your Airflow instance. It provides a controlled environment with all necessary dependencies. You can customize this file to include additional commands or override runtime settings.

- **include/**  
  Place any auxiliary files (such as SQL scripts, configuration files, or static resources) that your DAGs or other project components require in this folder.

- **packages.txt**  
  List any OS-level packages that your project requires. This file is used during the build process of your Docker image.

- **requirements.txt**  
  Specify the Python packages your project depends on. When you build your Docker image or deploy your code, these packages will be installed.

- **plugins/**  
  Add any custom or third-party Airflow plugins in this directory to extend Airflow’s capabilities.

- **airflow_settings.yaml**  
  Use this file to define Airflow Connections, Variables, and Pools locally. This can streamline your development process by avoiding manual configuration via the Airflow UI.

# Ingestion and Transformation Code

### Ingestion DAG – `historical_weather_ingestion`
- **Purpose:**  
  This DAG is responsible for fetching historical weather data from the Open-Meteo API and loading it into a staging table in PostgreSQL.

- **Implementation Highlights:**  
  - **Data Extraction:**  
    Uses Airflow’s `HttpHook` to make a GET request to the Open-Meteo Archive API, retrieving JSON data for a specified latitude, longitude, and date range.
  - **Data Loading:**  
    Stores the retrieved JSON data in a PostgreSQL table named `raw_historical_weather_data`.
  - **Duplicate Check:**  
    Compares the new JSON data (with sorted keys) against the most recent record to ensure only new data is loaded.
  - **Scheduling:**  
    Scheduled to run daily at 07:00 AM.

### Transformation DAG – `historical_weather_transformation`
- **Purpose:**  
  This DAG transforms the raw weather data from the ingestion phase into a structured format, then loads it into a final PostgreSQL table.

- **Implementation Highlights:**  
  - **Data Extraction:**  
    Retrieves the latest raw record along with its ingestion timestamp from the staging table.
  - **Data Transformation:**  
    Parses the raw JSON to extract hourly temperature data and attaches the ingestion timestamp for traceability.
  - **Data Loading:**  
    Inserts the transformed data into the `historical_weather_data` table only if records with the same ingestion timestamp do not already exist.
  - **Scheduling:**  
    Scheduled to run daily at 07:30 AM.

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
   Open your web browser and navigate to [http://localhost:8080/]. Log in with the default credentials (`admin`/`admin`). Here, you can view, trigger, and monitor your DAGs.

4. **Access the PostgreSQL Database:**  
   Your Airflow metadata is stored in a PostgreSQL database, accessible at `localhost:5432/postgres`.
