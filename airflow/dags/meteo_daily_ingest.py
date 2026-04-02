from airflow.sdk import dag, task
from datetime import datetime
import openmeteo_requests
import pandas as pd
import logging
import os
from io import BytesIO
from airflow.sdk import get_current_context
from airflow.providers.standard.operators.bash import BashOperator
from azure.storage.blob import BlobServiceClient

log = logging.getLogger(__name__)

AZURE_STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.environ.get("AZURE_STORAGE_KEY")


def get_blob_client():
    return BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net",
        credential=AZURE_STORAGE_KEY
    )


@dag(schedule="45 13 * * *", start_date=datetime(2026, 2, 28), catchup=False)
def meteo_daily_ingest():

    @task
    def fetch_meteo() -> dict:
        context = get_current_context()
        ds = context["ds"]

        raw_blob_name     = f"meteo/{ds}_meteo_data.json"
        parquet_blob_name = f"meteo/{ds}_meteo_data.parquet"

        # Idempotence — vérifie si le parquet existe déjà sur Blob
        client = get_blob_client()
        blob_parquet = client.get_blob_client(container="processed", blob=parquet_blob_name)
        if blob_parquet.exists():
            log.info(f"Déjà traité : {parquet_blob_name}")
            return {"parquet_blob_name": parquet_blob_name, "date": ds}

        centroides = pd.read_csv("/opt/airflow/dbt/seeds/departements_centroides.csv")

        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude":   centroides["lat"].tolist(),
            "longitude":  centroides["lon"].tolist(),
            "start_date": ds,
            "end_date":   ds,
            "daily": [
                "temperature_2m_mean",
                "temperature_2m_max",
                "temperature_2m_min",
                "rain_sum",
                "wind_speed_10m_max",
                "precipitation_sum",
                "weathercode",
            ],
        }

        om = openmeteo_requests.Client()
        responses = om.weather_api(url, params=params)

        rows = []
        for i, response in enumerate(responses):
            daily = response.Daily()
            rows.append({
                "date_ech":          ds,
                "code_departement":  centroides.iloc[i]["code_departement"],
                "latitude":          centroides.iloc[i]["lat"],
                "longitude":         centroides.iloc[i]["lon"],
                "temperature_mean":  daily.Variables(0).ValuesAsNumpy()[0],
                "temperature_max":   daily.Variables(1).ValuesAsNumpy()[0],
                "temperature_min":   daily.Variables(2).ValuesAsNumpy()[0],
                "rain_sum":          daily.Variables(3).ValuesAsNumpy()[0],
                "wind_speed_max":    daily.Variables(4).ValuesAsNumpy()[0],
                "precipitation_sum": daily.Variables(5).ValuesAsNumpy()[0],
                "weathercode":       daily.Variables(6).ValuesAsNumpy()[0],
            })

        df = pd.DataFrame(rows)

        # Upload JSON raw sur Blob
        blob_raw = client.get_blob_client(container="raw", blob=raw_blob_name)
        blob_raw.upload_blob(df.to_json(orient="records"), overwrite=True)

        # Upload Parquet processed sur Blob
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        blob_parquet.upload_blob(buffer, overwrite=True)
        log.info(f"Parquet uploadé : {parquet_blob_name}")

        return {"parquet_blob_name": parquet_blob_name, "date": ds}

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
            dbt run \
                --profiles-dir /opt/airflow/dbt \
                --project-dir /opt/airflow/dbt \
                --select stg_meteo_daily mart_atmo_meteo_daily_departement
        """,
        env={
            "DUCKDB_PATH": "/opt/data/analytics/atmo.duckdb",
            "PROCESSED_PATH": f"abfs://processed@{AZURE_STORAGE_ACCOUNT or ''}.blob.core.windows.net",
            "AZURE_STORAGE_ACCOUNT": AZURE_STORAGE_ACCOUNT or "",
            "AZURE_STORAGE_KEY": AZURE_STORAGE_KEY or "",
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
            "HOME": "/home/airflow",
        },
    )

    data = fetch_meteo()
    data >> run_dbt_models

meteo_daily_ingest()