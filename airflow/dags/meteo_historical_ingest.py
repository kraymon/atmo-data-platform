from airflow.sdk import dag, task
from datetime import datetime, date, timedelta, timezone
import openmeteo_requests
import pandas as pd
import numpy as np
import logging
import os
from io import BytesIO
from azure.storage.blob import BlobServiceClient

log = logging.getLogger(__name__)

# Configuration Azure (récupérée depuis les variables d'environnement comme ton autre DAG)
AZURE_STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.environ.get("AZURE_STORAGE_KEY")

def get_blob_client():
    return BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net",
        credential=AZURE_STORAGE_KEY
    )

@dag(schedule=None, start_date=datetime(2026, 2, 28), catchup=False)
def meteo_historical_ingest():

    @task
    def fetch_meteo_historical() -> dict:
        # Note : Le fichier seeds reste en local car il fait partie de ton repo dbt/airflow
        centroides = pd.read_csv("/opt/airflow/dbt/seeds/departements_centroides.csv")

        start_date = "2026-02-28"
        end_date   = "2026-04-01"

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

        log.info(f"Appel API Open-Meteo pour {len(centroides)} points")
        om = openmeteo_requests.Client()
        responses = om.weather_api(url, params=params)

        rows = []
        for i, response in enumerate(responses):
            daily = response.Daily()
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
        client = get_blob_client()
        
        saved, skipped = 0, 0
        
        # Itération par date pour l'idempotence sur Azure
        for ds, group in df.groupby("date_ech"):
            # Chemins des blobs (on enlève /opt/data et on définit le container)
            parquet_blob_name = f"meteo/{ds}_meteo_data.parquet"
            json_blob_name    = f"meteo/{ds}_meteo_data.json"

            # Vérification d'existence sur Azure (Container "processed")
            blob_parquet = client.get_blob_client(container="processed", blob=parquet_blob_name)
            
            if blob_parquet.exists():
                log.info(f"Azure Blob déjà présent : {parquet_blob_name} - skip")
                skipped += 1
                continue

            # Sauvegarde PARQUET vers container "processed"
            pq_buffer = BytesIO()
            group.to_parquet(pq_buffer, index=False, compression="snappy")
            pq_buffer.seek(0)
            blob_parquet.upload_blob(pq_buffer, overwrite=True)

            # Sauvegarde JSON vers container "raw"
            json_buffer = BytesIO()
            # On convertit en string d'abord pour l'upload blob
            json_data = group.to_json(orient="records")
            blob_raw = client.get_blob_client(container="raw", blob=json_blob_name)
            blob_raw.upload_blob(json_data, overwrite=True)

            log.info(f"Upload réussi pour le {ds}")
            saved += 1

        log.info(f"Terminé : {saved} jours uploadés, {skipped} déjà présents sur Azure")
        return {"saved": saved, "skipped": skipped}

    fetch_meteo_historical()

meteo_historical_ingest()