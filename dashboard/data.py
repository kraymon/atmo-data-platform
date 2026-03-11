import streamlit as st
import duckdb
import requests
from config import DUCKDB_PATH, GEOJSON_URL


@st.cache_resource
def get_connection():
    return duckdb.connect(DUCKDB_PATH, read_only=True)


@st.cache_data(ttl=300)
def load_national(_con):
    return _con.sql("""
        SELECT date_ech, indice_moyen_national, nb_communes,
               nb_bon, nb_mauvais_ou_pire, polluants_dominants_national
        FROM mart_atmo_daily_national
        ORDER BY date_ech DESC
    """).df()


@st.cache_data(ttl=300)
def load_departement(_con):
    return _con.sql("""
        SELECT date_ech, code_departement, indice_moyen,
               nb_communes, polluants_dominants_departement,
               pct_no2_declencheur, pct_o3_declencheur,
               pct_pm10_declencheur, pct_pm25_declencheur, pct_so2_declencheur
        FROM mart_atmo_daily_departement
        ORDER BY date_ech DESC
    """).df()


@st.cache_data(ttl=300)
def load_commune(_con):
    return _con.sql("""
        SELECT code_insee, nom_commune, code_departement,
               indice_moyen, pct_jours_acceptable,
               polluants_dominants, nb_jours_mesures,
               premiere_date, derniere_date,
               indice_moyen_no2, indice_moyen_o3,
               indice_moyen_pm10, indice_moyen_pm25, indice_moyen_so2
        FROM mart_atmo_commune
        WHERE nb_jours_mesures >= 2
    """).df()


@st.cache_data(ttl=3600)
def load_geojson():
    r = requests.get(GEOJSON_URL, timeout=10)
    return r.json()


def load_all():
    con = get_connection()
    return {
        "con":          con,
        "national":     load_national(con),
        "departement":  load_departement(con),
        "commune":      load_commune(con),
    }