from airflow.sdk import dag, task
from datetime import date, datetime
import openmeteo_requests
import pandas as pd
import logging
import os, json
import pandas as pd
from airflow.sdk import get_current_context
from airflow.providers.standard.operators.bash import BashOperator

log = logging.getLogger(__name__)

@dag(schedule="45 13 * * *", start_date=datetime(2026, 2, 28), catchup=False)
def meteo_daily_ingest():

    @task
    def fetch_meteo() -> dict:
        context = get_current_context()
        ds = context["ds"]

        centroides = pd.read_csv("/opt/airflow/dbt/seeds/departements_centroides.csv")

        raw_path     = f"/opt/data/raw/meteo/{ds}_meteo_data.json"
        parquet_path = f"/opt/data/processed/meteo/{ds}_meteo_data.parquet"

        if os.path.exists(parquet_path):
            log.info(f"Déjà traité : {parquet_path}")
            return {"path": parquet_path, "date": ds}

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

        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

        df.to_json(raw_path, orient="records")
        df.to_parquet(parquet_path, index=False)

        return {"path": parquet_path, "date": ds}

    fetch_meteo()

meteo_daily_ingest()