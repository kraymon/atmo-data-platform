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

log = logging.getLogger(__name__)

BASE_DATA_PATH = "/opt/data"


@dag(schedule="0 13 * * *", start_date=datetime(2026, 2, 28), catchup=False)
def atmo_daily_ingest():

    @task
    def download_data():

        url = "https://www.data.gouv.fr/api/1/datasets/r/d2b9e8e6-8b0b-4bb6-9851-b4fa2efc8201"

        # use date to create daily files
        context = get_current_context()
        date = context["ds"]
        local_path = f"{BASE_DATA_PATH}/raw/atmo/{date}_atmo_data.csv"

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Idempotence
        if os.path.exists(local_path):
            log.info(f"Déjà téléchargé : {local_path}")
            return {"path": local_path, "date": date}

        log.info(f"Téléchargement: {url}")

        r = requests.get(url, timeout=30)
        r.raise_for_status()

        with open(local_path, "wb") as f:
            f.write(r.content)

        log.info("Download finished")

        return { "path" : local_path, "date" : date }
    
    @task
    def validate_schema(data):

        csv_path = data["path"]
        date = data["date"]

        df = pd.read_csv(csv_path, sep=",", nrows=5)
        # log the columns for debugging
        log.info(f"Columns: {df.columns.tolist()}")

        # excepted number of collumns is 22
        if len(df.columns) != 22: 
            raise ValueError(f"Expected 22 columns, got {len(df.columns)}")
        
        log.info("Schema validation passed")

        return data

    @task
    def csv_to_parquet(data):
        csv_path = data["path"]
        date = data["date"]

        df = pd.read_csv(csv_path, sep=",", dtype={"code_zone": str})
        
        df = df[df["date_ech"] == date].copy()

        if len(df) == 0:
            raise ValueError(f"0 lignes pour {date} — vérifier le fichier source")

        log.info(f"Lignes pour {date} : {len(df)}")

        parquet_path = csv_path.replace("raw", "processed").replace(".csv", ".parquet")

        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        df.to_parquet(parquet_path, index=False, compression="snappy")
        log.info(f"Saved parquet file to: {parquet_path}")

        return { "path" : parquet_path, "date" : date }

    
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
            dbt run \
                --profiles-dir /opt/airflow/dbt \
                --project-dir /opt/airflow/dbt \
                --select stg_atmo_daily mart_atmo_commune mart_atmo_daily_national mart_atmo_daily_departement mon_communes_coverage mon_departements_manquants
        """,
        env={
            "DUCKDB_PATH": "/opt/data/analytics/atmo.duckdb",
            "PROCESSED_PATH": "/opt/data/processed",
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
    )

    @task
    def check_monitoring(_):
        con = duckdb.connect("/opt/data/analytics/atmo.duckdb", read_only=True)

        coverage = con.sql("""
            SELECT statut, nb_communes_manquantes, nb_communes_hier
            FROM mon_commune_coverage
        """).df()

        manquants = con.sql("""
            SELECT statut_global, nb_departements_manquants
            FROM mon_departement_manquant
            WHERE statut_global != 'OK'
        """).df()

        con.close()

        alertes = []

        if not coverage.empty and coverage["statut"].iloc[0] == "CRITICAL":
            n = int(coverage["nb_communes_manquantes"].iloc[0])
            total = int(coverage["nb_communes_hier"].iloc[0])
            alertes.append(f"Couverture communes CRITICAL : {n} manquantes vs hier ({total})")

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