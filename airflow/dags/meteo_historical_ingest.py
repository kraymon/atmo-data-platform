from airflow.sdk import dag, task
from datetime import datetime, date, timedelta, timezone
import openmeteo_requests
import pandas as pd
import numpy as np
import logging
import os

log = logging.getLogger(__name__)

@dag(schedule=None, start_date=datetime(2026, 2, 28), catchup=False)
def meteo_historical_ingest():

    @task
    def fetch_meteo_historical() -> dict:
        centroides = pd.read_csv("/opt/airflow/dbt/seeds/departements_centroides.csv")


        start_date = "2026-02-28"
        end_date   = "2026-03-13"

        url = "https://archive-api.open-meteo.com/v1/archive"

        params = {
            "latitude":   centroides["lat"].tolist(),
            "longitude":  centroides["lon"].tolist(),
            "start_date": start_date,
            "end_date":   end_date,
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

        log.info(f"Backfill météo du {start_date} au {end_date}")

        om = openmeteo_requests.Client()
        responses = om.weather_api(url, params=params)

        # Reconstruire toutes les lignes : N départements × N jours
        rows = []
        for i, response in enumerate(responses):
            daily = response.Daily()

            # Reconstruire la liste des dates depuis l'interval retourné
            nb_jours = daily.Variables(0).ValuesAsNumpy().shape[0]
            first_date = datetime.fromtimestamp(daily.Time(), tz=timezone.utc).date()

            for j in range(nb_jours):
                ds = str(first_date + timedelta(days=j))
                rows.append({
                    "date_ech":          ds,
                    "code_departement":  centroides.iloc[i]["code_departement"],
                    "latitude":          centroides.iloc[i]["lat"],
                    "longitude":         centroides.iloc[i]["lon"],
                    "temperature_mean":  daily.Variables(0).ValuesAsNumpy()[j],
                    "temperature_max":   daily.Variables(1).ValuesAsNumpy()[j],
                    "temperature_min":   daily.Variables(2).ValuesAsNumpy()[j],
                    "rain_sum":          daily.Variables(3).ValuesAsNumpy()[j],
                    "wind_speed_max":    daily.Variables(4).ValuesAsNumpy()[j],
                    "precipitation_sum": daily.Variables(5).ValuesAsNumpy()[j],
                    "weathercode":       daily.Variables(6).ValuesAsNumpy()[j],
                })

        df = pd.DataFrame(rows)

        # Sauvegarder un Parquet par jour - avec idempotence
        saved, skipped = 0, 0
        for ds, group in df.groupby("date_ech"):
            parquet_path = f"/opt/data/processed/meteo/{ds}_meteo_data.parquet"
            raw_path     = f"/opt/data/raw/meteo/{ds}_meteo_data.json"

            if os.path.exists(parquet_path):
                log.info(f"Déjà traité : {ds} - skip")
                skipped += 1
                continue

            os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
            os.makedirs(os.path.dirname(raw_path), exist_ok=True)

            group.to_parquet(parquet_path, index=False)
            group.to_json(raw_path, orient="records")
            saved += 1

        log.info(f"Backfill terminé : {saved} jours sauvegardés, {skipped} ignorés")
        return {"saved": saved, "skipped": skipped}

    fetch_meteo_historical()

meteo_historical_ingest()