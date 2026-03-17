import streamlit as st
from styles import inject_styles
from data import load_all
from components.hero import render_hero
from components.carte import render_carte
from components.tendance import render_tendance
from components.communes import render_communes
from components.meteo_atmo import render_meteo_atmo
from components.monitoring import render_monitoring
from components.sql_explorer import render_sql_explorer

st.set_page_config(
    page_title="ATMO · Qualité de l'air",
    layout="wide",
    initial_sidebar_state="collapsed"
)

inject_styles()
data = load_all()
render_hero(data)

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "CARTE · DÉPARTEMENTS",
    "TENDANCE NATIONALE",
    "CLASSEMENT COMMUNES",
    "MÉTÉO · CORRÉLATIONS",
    "MONITORING",
    "EXPLORATEUR SQL",
])

with tab1: render_carte(data)
with tab2: render_tendance(data)
with tab3: render_communes(data)
with tab4: render_meteo_atmo(data)
with tab5: render_monitoring(data)
with tab6: render_sql_explorer(data)