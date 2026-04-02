import streamlit as st
import duckdb
import requests
import os
import adlfs
from config import DUCKDB_PATH, GEOJSON_URL

import time

def execute_query(query: str):
    account = os.environ.get("AZURE_STORAGE_ACCOUNT")
    key = os.environ.get("AZURE_STORAGE_KEY")
    
    # Retry logic if Airflow/dbt is currently holding the write lock
    max_retries = 30
    last_error = None
    for i in range(max_retries):
        try:
            with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
                if account and key:
                    fs = adlfs.AzureBlobFileSystem(account_name=account, account_key=key)
                    con.register_filesystem(fs)
                return con.sql(query).df()
        except duckdb.IOException as e:
            last_error = e
            # Wait 2 seconds and retry if it's a lock error from Airflow writing
            time.sleep(2)
        except Exception as e:
            raise e
            
    raise Exception(f"DuckDB database locked continuously after {max_retries} retries. Error: {last_error}")


@st.cache_data(ttl=300)
def load_national():
    return execute_query("""
        SELECT date_ech, indice_moyen_national, nb_communes,
               nb_bon, nb_mauvais_ou_pire, polluants_dominants_national
        FROM mart_atmo_daily_national
        ORDER BY date_ech DESC
    """)


@st.cache_data(ttl=300)
def load_departement():
    return execute_query("""
        SELECT date_ech, code_departement, indice_moyen,
               nb_communes, polluants_dominants_departement,
               pct_no2_declencheur, pct_o3_declencheur,
               pct_pm10_declencheur, pct_pm25_declencheur, pct_so2_declencheur
        FROM mart_atmo_daily_departement
        ORDER BY date_ech DESC
    """)


@st.cache_data(ttl=300)
def load_commune():
    return execute_query("""
        SELECT code_insee, nom_commune, code_departement,
               indice_moyen, pct_jours_acceptable,
               polluants_dominants, nb_jours_mesures,
               premiere_date, derniere_date,
               indice_moyen_no2, indice_moyen_o3,
               indice_moyen_pm10, indice_moyen_pm25, indice_moyen_so2
        FROM mart_atmo_commune
        WHERE nb_jours_mesures >= 2
    """)


@st.cache_data(ttl=3600)
def load_geojson():
    r = requests.get(GEOJSON_URL, timeout=10)
    return r.json()


@st.cache_data(ttl=300)
def load_monitoring():
    return execute_query("SELECT * FROM mon_departement_manquant ORDER BY code_departement")


@st.cache_data(ttl=300)
def load_meteo_atmo():
    return execute_query("""
        SELECT
            date_ech,
            round(avg(temperature_max), 1) as temperature_max,
            round(avg(indice_moyen), 2)     as indice_moyen
        FROM mart_atmo_meteo_daily_departement
        WHERE temperature_max IS NOT NULL
        GROUP BY date_ech
        ORDER BY date_ech
    """)


def load_all():
    return {
        "con":          None,  # Supprimé pour éviter les conflits d'accès
        "national":     load_national(),
        "departement":  load_departement(),
        "commune":      load_commune(),
        "monitoring":   load_monitoring(),
        "meteo_atmo":   load_meteo_atmo(),
    }