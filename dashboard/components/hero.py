import streamlit as st


def render_hero(data: dict):
    df_national   = data["national"]
    derniere_date = df_national["date_ech"].iloc[0] if not df_national.empty else "—"
    indice_today  = df_national["indice_moyen_national"].iloc[0] if not df_national.empty else "—"
    nb_communes   = int(df_national["nb_communes"].iloc[0]) if not df_national.empty else 0
    nb_bon        = int(df_national["nb_bon"].iloc[0]) if not df_national.empty else 0
    nb_mauvais    = int(df_national["nb_mauvais_ou_pire"].iloc[0]) if not df_national.empty else 0
    pct_bon       = round(100 * nb_bon / nb_communes, 1) if nb_communes else 0

    st.markdown(f"""
        <div class="hero">
            <div>
                <div class="hero-title">ATMO<span>·</span>AIR</div>
                <div class="hero-sub">Observatoire national de la qualité de l'air · France métropolitaine & DOM</div>
            </div>
            <div class="live-badge">
                <div class="live-dot"></div>
                Mis à jour · {str(derniere_date)[:10]}
            </div>
        </div>

        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-label">Indice national · {str(derniere_date)[:10]}</div>
                <div class="kpi-value">{indice_today}</div>
                <div class="kpi-sub">sur 6 · moyenne pondérée</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Communes surveillées</div>
                <div class="kpi-value">{nb_communes:,}</div>
                <div class="kpi-sub">zones de mesure actives</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Qualité Bonne</div>
                <div class="kpi-value" style="color:var(--good)">{pct_bon}%</div>
                <div class="kpi-sub">{nb_bon:,} communes · indice = 1</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-label">Qualité Mauvaise ou pire</div>
                <div class="kpi-value" style="color:var(--bad)">{nb_mauvais:,}</div>
                <div class="kpi-sub">communes · indice ≥ 4</div>
            </div>
        </div>
    """, unsafe_allow_html=True)