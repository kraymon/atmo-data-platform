import streamlit as st


_TABLES = [
    "stg_atmo_daily",
    "stg_meteo_daily",
    "mart_atmo_commune",
    "mart_atmo_daily_national",
    "mart_atmo_daily_departement",
    "mart_atmo_meteo_daily_departement",
]

_DEFAULT_QUERY = "SELECT * FROM mart_atmo_daily_national ORDER BY date_ech DESC LIMIT 10"


def render_sql_explorer(data: dict):
    con = data["con"]

    st.markdown(f"""
        <div class="section-header">
            <span class="section-title">Console SQL · DuckDB</span>
            <div class="section-line"></div>
        </div>
        <div style="font-family:'DM Mono',monospace;font-size:0.7rem;color:var(--muted);margin-bottom:1rem">
            Tables disponibles : {" · ".join(_TABLES)}
        </div>
    """, unsafe_allow_html=True)

    query = st.text_area(
        "",
        value=_DEFAULT_QUERY,
        height=160,
        placeholder="SELECT ...",
        key="sql_query",
    )

    col_btn, _ = st.columns([1, 4])
    with col_btn:
        run = st.button("⏎  Exécuter", use_container_width=True)

    if run and query.strip():
        try:
            df_q = con.sql(query).df()

            st.markdown(f"""
                <div style="font-family:'DM Mono',monospace;font-size:0.7rem;
                            color:var(--accent);margin-bottom:0.5rem">
                    ✓ {len(df_q)} lignes · {len(df_q.columns)} colonnes
                </div>
            """, unsafe_allow_html=True)

            st.dataframe(df_q, use_container_width=True)

            st.download_button(
                "↓ Export CSV",
                data=df_q.to_csv(index=False).encode("utf-8"),
                file_name="export_atmo.csv",
                mime="text/csv",
            )

        except Exception as e:
            st.markdown(f"""
                <div style="font-family:'DM Mono',monospace;font-size:0.75rem;
                            color:#F87171;padding:1rem;border:1px solid #F87171;
                            background:rgba(248,113,113,0.05)">
                    ✗ Erreur SQL : {e}
                </div>
            """, unsafe_allow_html=True)