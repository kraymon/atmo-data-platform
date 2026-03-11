import streamlit as st
import plotly.express as px
from data import load_geojson
from config import COULEURS_INDICE, COLOR_SCALE, PLOT_BG, MUTED_COLOR, FONT_MONO


def render_carte(data: dict):
    df_departement = data["departement"]

    col_ctrl, col_map = st.columns([1, 3])

    with col_ctrl:
        dates_dispo = sorted(df_departement["date_ech"].unique(), reverse=True)
        dates_str   = [str(d)[:10] for d in dates_dispo]
        date_sel    = st.selectbox("Date", dates_str)

        df_day = df_departement[
            df_departement["date_ech"].astype(str).str[:10] == date_sel
        ].copy()

        # Indice moyen du jour
        indice_jour = round(df_day["indice_moyen"].mean(), 2)
        st.markdown(f"""
            <div style="margin-top:1.5rem">
                <div class="kpi-label">Indice moyen · {date_sel}</div>
                <div class="kpi-value">{indice_jour}</div>
            </div>
            <div style="margin-top:1rem">
                <div class="kpi-label">Polluant dominant</div>
            </div>
        """, unsafe_allow_html=True)

        # Polluant le plus fréquent ce jour
        polluants_flat = []
        for p in df_day["polluants_dominants_departement"].dropna():
            if isinstance(p, list):
                polluants_flat.extend(p)
        if polluants_flat:
            dominant = max(set(polluants_flat), key=polluants_flat.count)
            st.markdown(f'<span class="tag">{dominant}</span>', unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

        # Top 5 pires départements
        st.markdown("""
            <div style="margin-top:1.5rem">
                <div class="kpi-label">Top 5 · pires dép.</div>
            </div>
        """, unsafe_allow_html=True)

        df_top5 = df_day.nlargest(5, "indice_moyen")[["code_departement", "indice_moyen"]]
        for _, row in df_top5.iterrows():
            couleur = COULEURS_INDICE.get(round(row["indice_moyen"]), "#4A5568")
            st.markdown(f"""
                <div style="display:flex;justify-content:space-between;align-items:center;
                            padding:0.4rem 0;border-bottom:1px solid var(--border)">
                    <span style="font-family:'DM Mono',monospace;font-size:0.8rem">
                        Dép. {row['code_departement']}
                    </span>
                    <span style="font-family:'DM Mono',monospace;font-size:0.85rem;
                                font-weight:600;color:{couleur}">
                        {row['indice_moyen']:.2f}
                    </span>
                </div>
            """, unsafe_allow_html=True)

    with col_map:
        try:
            geojson = load_geojson()

            fig_map = px.choropleth(
                df_day,
                geojson=geojson,
                locations="code_departement",
                featureidkey="properties.code",
                color="indice_moyen",
                color_continuous_scale=COLOR_SCALE,
                range_color=[1, 6],
                hover_name="code_departement",
                hover_data={"indice_moyen": ":.2f", "nb_communes": True},
                labels={"indice_moyen": "Indice", "nb_communes": "Communes"},
            )

            fig_map.update_geos(
                fitbounds="locations",
                visible=False,
                bgcolor="#080C10",
                projection_type="mercator",
            )

            fig_map.update_layout(
                paper_bgcolor="#080C10",
                plot_bgcolor="#080C10",
                margin={"r": 0, "t": 0, "l": 0, "b": 0},
                height=520,
                coloraxis_colorbar=dict(
                    title="Indice",
                    tickfont=dict(color=MUTED_COLOR, family=FONT_MONO, size=10),
                    bgcolor="#0E1318",
                    bordercolor="#1C2530",
                    borderwidth=1,
                    tickvals=[1, 2, 3, 4, 5, 6],
                    ticktext=["Bon", "Moyen", "Dégradé", "Mauvais", "Très mauvais", "Extrême"],
                ),
            )

            st.plotly_chart(fig_map, use_container_width=True)

        except Exception as e:
            st.error(f"Carte indisponible : {e}")
            st.dataframe(
                df_day[["code_departement", "indice_moyen", "nb_communes"]],
                use_container_width=True,
            )