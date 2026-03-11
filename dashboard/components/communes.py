import streamlit as st


def render_communes(data: dict):
    df_commune = data["commune"]

    col_f, col_f2 = st.columns(2)

    with col_f:
        st.markdown("""
            <div class="section-header">
                <span class="section-title" style="color:var(--good)">● Top 15 · Meilleur air</span>
                <div class="section-line"></div>
            </div>
        """, unsafe_allow_html=True)

        df_top = (
            df_commune[df_commune["nom_commune"].notna()]
            .nsmallest(15, "indice_moyen")
            [["nom_commune", "code_departement", "indice_moyen", "pct_jours_acceptable"]]
        )
        df_top.columns = ["Commune", "Dép.", "Indice moy.", "% jours acceptable"]

        st.dataframe(
            df_top.style.background_gradient(
                subset=["Indice moy."],
                cmap="RdYlGn_r", vmin=1, vmax=6,
            ),
            use_container_width=True,
            hide_index=True,
        )

    with col_f2:
        st.markdown("""
            <div class="section-header">
                <span class="section-title" style="color:var(--bad)">● Top 15 · Pire air</span>
                <div class="section-line"></div>
            </div>
        """, unsafe_allow_html=True)

        df_flop = (
            df_commune[df_commune["nom_commune"].notna()]
            .nlargest(15, "indice_moyen")
            [["nom_commune", "code_departement", "indice_moyen", "pct_jours_acceptable"]]
        )
        df_flop.columns = ["Commune", "Dép.", "Indice moy.", "% jours acceptable"]

        st.dataframe(
            df_flop.style.background_gradient(
                subset=["Indice moy."],
                cmap="RdYlGn_r", vmin=1, vmax=6,
            ),
            use_container_width=True,
            hide_index=True,
        )

    # Recherche
    st.markdown("""
        <div class="section-header" style="margin-top:2rem">
            <span class="section-title">Recherche commune</span>
            <div class="section-line"></div>
        </div>
    """, unsafe_allow_html=True)

    search = st.text_input("", placeholder="Nom de commune ou code INSEE...", key="search_commune")

    if search:
        mask = (
            df_commune["nom_commune"].str.lower().str.contains(search.lower(), na=False) |
            df_commune["code_insee"].astype(str).str.contains(search)
        )
        df_search = df_commune[mask].sort_values("indice_moyen")[[
            "nom_commune", "code_insee", "code_departement",
            "indice_moyen", "pct_jours_acceptable",
            "indice_moyen_no2", "indice_moyen_o3",
            "indice_moyen_pm10", "indice_moyen_pm25",
        ]]
        df_search.columns = [
            "Commune", "INSEE", "Dép.", "Indice moy.",
            "% acceptable", "NO2", "O3", "PM10", "PM2.5",
        ]

        st.dataframe(
            df_search.style.background_gradient(
                subset=["Indice moy."],
                cmap="RdYlGn_r", vmin=1, vmax=6,
            ),
            use_container_width=True,
            hide_index=True,
        )