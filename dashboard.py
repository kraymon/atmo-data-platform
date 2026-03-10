import streamlit as st
import duckdb
import pandas as pd

DUCKDB_PATH = "./data/analytics/atmo.duckdb"

st.set_page_config(page_title="Qualité de l'air - France", layout="wide")
st.title("🌬️ Qualité de l'air en France")

con = duckdb.connect(DUCKDB_PATH, read_only=True)


# ─── Explorateur SQL ──────────────────────────────────────────────
st.header("Explorateur SQL")

query = st.text_area(
    "Requête SQL",
    value="SELECT * FROM mart_atmo_daily_national LIMIT 10",
    height=150
)

if st.button("Exécuter"):
    try:
        df_query = con.sql(query).df()
        st.dataframe(df_query, use_container_width=True)
    except Exception as e:
        st.error(f"Erreur : {e}")

# ─── National par jour ───────────────────────────────────────────
st.header("Vue nationale")

df_national = con.sql("""
    SELECT date_ech, indice_moyen_national, nb_communes,
           nb_bon, nb_mauvais_ou_pire, polluants_dominants_national
    FROM mart_atmo_daily_national
    ORDER BY date_ech DESC
""").df()

st.dataframe(df_national, use_container_width=True)

# Affichage de l'évolution de l'indice moyen national par jour
df_national["date_ech"] = df_national["date_ech"].astype(str)
st.line_chart(df_national.set_index("date_ech")["indice_moyen_national"], height=200)

# ─── Recherche par département ───────────────────────────────────────────
st.header("Vue départementale")

search_dept = st.text_input("Code de département (ex: 75)")

df_departement = con.sql(f"""
    SELECT date_ech, code_departement, indice_moyen,
           nb_communes, polluants_dominants_departement
    FROM mart_atmo_daily_departement
    WHERE code_departement LIKE '%{search_dept}%'
    ORDER BY date_ech DESC
""").df()

st.dataframe(df_departement, use_container_width=True)

# ─── Recherche par commune ────────────────────────────────────────
st.header("Recherche par commune")

search = st.text_input("Nom de commune ou code INSEE")

if search:
    df_commune = con.sql(f"""
        SELECT code_insee, nom_commune, indice_moyen,
               pct_jours_acceptable, polluants_dominants,
               nb_jours_mesures, premiere_date, derniere_date
        FROM mart_atmo_commune
        WHERE lower(nom_commune) LIKE lower('%{search}%')
           OR code_insee LIKE '%{search}%'
        ORDER BY indice_moyen ASC
        LIMIT 20
    """).df()

    st.dataframe(df_commune, use_container_width=True)

# ─── Classement communes ──────────────────────────────────────────
st.header("Classement communes")

col1, col2 = st.columns(2)

with col1:
    st.subheader("🟢 Top 10 meilleures")
    df_top = con.sql("""
        SELECT nom_commune, code_insee, indice_moyen, pct_jours_acceptable
        FROM mart_atmo_commune
        WHERE nb_jours_mesures >= 3
        ORDER BY indice_moyen ASC
        LIMIT 10
    """).df()
    st.dataframe(df_top, use_container_width=True)

with col2:
    st.subheader("🔴 Top 10 pires")
    df_flop = con.sql("""
        SELECT nom_commune, code_insee, indice_moyen, pct_jours_acceptable
        FROM mart_atmo_commune
        WHERE nb_jours_mesures >= 3
        ORDER BY indice_moyen DESC
        LIMIT 10
    """).df()
    st.dataframe(df_flop, use_container_width=True)

con.close()