from curses import raw
from airflow.sdk import dag, task
from datetime import date, datetime
import requests
import logging
import os
import pandas as pd
import duckdb
from airflow.sdk import get_current_context
from airflow.providers.standard.operators.bash import BashOperator
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import adlfs

log = logging.getLogger(__name__)

AZURE_STORAGE_ACCOUNT = os.environ.get("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.environ.get("AZURE_STORAGE_KEY")


def get_blob_client():
    return BlobServiceClient(
        account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net",
        credential=AZURE_STORAGE_KEY
    )


@dag(schedule="0 13 * * *", start_date=datetime(2026, 2, 28), catchup=False)
def atmo_daily_ingest():

    @task
    def download_data():
        url = "https://www.data.gouv.fr/api/1/datasets/r/d2b9e8e6-8b0b-4bb6-9851-b4fa2efc8201"
        context = get_current_context()
        date = context["ds"]
        blob_name = f"atmo/{date}_atmo_data.csv"

        # Idempotence — vérifie si le blob existe déjà
        client = get_blob_client()
        blob = client.get_blob_client(container="raw", blob=blob_name)
        if blob.exists():
            log.info(f"Déjà téléchargé : {blob_name}")
            return {"blob_name": blob_name, "date": date}

        log.info(f"Téléchargement: {url}")
        r = requests.get(url, timeout=30)
        r.raise_for_status()

        blob.upload_blob(r.content, overwrite=True)
        log.info(f"Uploadé sur Blob : {blob_name}")

        return {"blob_name": blob_name, "date": date}

    @task
    def validate_schema(data):
        client = get_blob_client()
        blob = client.get_blob_client(container="raw", blob=data["blob_name"])
        content = blob.download_blob().readall()
        df = pd.read_csv(BytesIO(content), sep=",", nrows=5)
        log.info(f"Columns: {df.columns.tolist()}")
        if len(df.columns) != 22:
            raise ValueError(f"Expected 22 columns, got {len(df.columns)}")
        log.info("Schema validation passed")
        return data

    @task
    def csv_to_parquet(data):
        date = data["date"]
        client = get_blob_client()

        # Lire le CSV depuis Blob
        blob_csv = client.get_blob_client(container="raw", blob=data["blob_name"])
        content = blob_csv.download_blob().readall()
        df = pd.read_csv(BytesIO(content), sep=",", dtype={"code_zone": str})
        df = df[df["date_ech"] == date].copy()

        if len(df) == 0:
            raise ValueError(f"0 lignes pour {date}")

        log.info(f"Lignes pour {date} : {len(df)}")

        # Écrire le Parquet sur Blob
        parquet_blob_name = f"atmo/{date}_atmo_data.parquet"
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, compression="snappy")
        buffer.seek(0)

        blob_parquet = client.get_blob_client(container="processed", blob=parquet_blob_name)
        blob_parquet.upload_blob(buffer, overwrite=True)
        log.info(f"Parquet uploadé : {parquet_blob_name}")

        return {"parquet_blob_name": parquet_blob_name, "date": date}

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
            dbt run \
                --profiles-dir /opt/airflow/dbt \
                --project-dir /opt/airflow/dbt \
                --select stg_atmo_daily mart_atmo_commune mart_atmo_daily_national mart_atmo_daily_departement mart_atmo_daily_commune mon_commune_coverage mon_departement_manquant
        """,
        env={
            "DUCKDB_PATH": "/opt/data/analytics/atmo.duckdb",
            "PROCESSED_PATH": f"abfs://processed@{AZURE_STORAGE_ACCOUNT or ''}.blob.core.windows.net",
            "AZURE_STORAGE_ACCOUNT": AZURE_STORAGE_ACCOUNT or "",
            "AZURE_STORAGE_KEY": AZURE_STORAGE_KEY or "",
            "HOME": "/home/airflow",
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
    )

    @task
    def check_monitoring(_):

        account = os.environ.get("AZURE_STORAGE_ACCOUNT")
        key = os.environ.get("AZURE_STORAGE_KEY")
        
        con = duckdb.connect("/opt/data/analytics/atmo.duckdb", read_only=False)
        fs = adlfs.AzureBlobFileSystem(account_name=account, account_key=key)
        con.register_filesystem(fs)

        manquants = con.sql("""
            SELECT statut_global, nb_departements_manquants
            FROM mon_departement_manquant
            WHERE statut_global != 'OK'
        """).df()

        con.close()

        alertes = []

        if not manquants.empty and manquants["statut_global"].iloc[0] == "CRITICAL":
            n = int(manquants["nb_departements_manquants"].iloc[0])
            alertes.append(f"Départements manquants CRITICAL : {n} absents vs hier")

        if alertes:
            raise ValueError("Monitoring ATMO :\n" + "\n".join(alertes))


    data = download_data()
    validate = validate_schema(data)
    parquet = csv_to_parquet(validate)
    parquet >> run_dbt_models
    check_monitoring(run_dbt_models.output)

atmo_daily_ingest()