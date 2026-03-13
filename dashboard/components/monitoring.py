import streamlit as st


def render_monitoring(data: dict):
    con = data["con"]

    df_coverage  = con.sql("SELECT * FROM mon_commune_coverage").df()
    df_manquants = con.sql("SELECT * FROM mon_departement_manquant ORDER BY code_departement").df()

    _render_status_banner(df_coverage, df_manquants)
    _render_coverage(df_coverage)
    _render_manquants(df_manquants)


def _statut_badge(statut: str) -> str:
    colors = {"OK": "#2DD4BF", "WARN": "#F59E0B", "CRITICAL": "#F87171"}
    color = colors.get(statut, "#4A5568")
    return (
        f'<span style="font-family:\'DM Mono\',monospace;font-size:0.7rem;font-weight:600;'
        f'color:{color};border:1px solid {color};padding:0.15rem 0.5rem;border-radius:2px;">'
        f'{statut}</span>'
    )


def _render_status_banner(df_coverage, df_manquants):
    statut_coverage  = df_coverage["statut"].iloc[0]        if not df_coverage.empty  else "—"
    statut_manquants = df_manquants["statut_global"].iloc[0] if not df_manquants.empty else "—"
    nb_manquants     = int(df_manquants["nb_departements_manquants"].iloc[0]) if not df_manquants.empty else 0
    nb_communes      = int(df_coverage["nb_communes_aujourd_hui"].iloc[0])    if not df_coverage.empty  else 0
    nb_hier          = int(df_coverage["nb_communes_hier"].iloc[0])           if not df_coverage.empty  else 0
    nb_absent        = int(df_coverage["nb_communes_manquantes"].iloc[0])     if not df_coverage.empty  else 0

    delta_str = f"{nb_absent:,} communes manquantes vs hier" if nb_absent > 0 else "couverture stable vs hier"

    global_statut = "OK"
    if "CRITICAL" in [statut_coverage, statut_manquants]:
        global_statut = "CRITICAL"
    elif "WARN" in [statut_coverage, statut_manquants]:
        global_statut = "WARN"

    banner_colors = {
        "OK":       ("rgba(45,212,191,0.05)",  "#2DD4BF"),
        "WARN":     ("rgba(245,158,11,0.05)",  "#F59E0B"),
        "CRITICAL": ("rgba(248,113,113,0.05)", "#F87171"),
    }
    bg, border = banner_colors.get(global_statut, ("rgba(74,85,104,0.05)", "#4A5568"))

    st.markdown(f"""
        <div style="background:{bg};border:1px solid {border};padding:1.25rem 1.5rem;
                    margin-bottom:2rem;display:flex;align-items:center;justify-content:space-between;">
            <div style="display:flex;align-items:center;gap:1rem">
                {_statut_badge(global_statut)}
                <span style="font-family:'DM Mono',monospace;font-size:0.75rem;color:#E2E8F0">
                    {nb_communes:,} communes aujourd'hui · {delta_str}
                </span>
            </div>
            <div style="font-family:'DM Mono',monospace;font-size:0.7rem;color:#4A5568">
                {nb_hier:,} communes hier · {nb_manquants} dép. manquants
            </div>
        </div>
    """, unsafe_allow_html=True)


 
def _render_coverage(df_coverage):
    st.markdown("""
        <div class="section-header">
            <span class="section-title">Couverture communes · J vs J-1</span>
            <div class="section-line"></div>
        </div>
    """, unsafe_allow_html=True)
 
    if df_coverage.empty:
        st.info("Aucune donnée de couverture disponible.")
        return
 
    row = df_coverage.iloc[0]
 
    def _metric_card(label, value, color="#E2E8F0"):
        return (
            f'<div style="padding:1rem 1.25rem;border:1px solid rgba(255,255,255,0.07);background:rgba(255,255,255,0.02)">'
            f'<div style="font-family:\'DM Mono\',monospace;font-size:0.65rem;color:#4A5568;text-transform:uppercase;letter-spacing:0.08em;margin-bottom:0.4rem">{label}</div>'
            f'<div style="font-family:\'DM Mono\',monospace;font-size:1.4rem;font-weight:600;color:{color}">{value}</div>'
            f'</div>'
        )
 
    statut_colors = {"OK": "#2DD4BF", "WARN": "#F59E0B", "CRITICAL": "#F87171"}
    statut_color  = statut_colors.get(row["statut"], "#E2E8F0")
    manquantes    = int(row["nb_communes_manquantes"])
    manq_color    = "#F87171" if manquantes > 0 else "#2DD4BF"
 
    col1, col2, col3, col4 = st.columns(4)
    col1.markdown(_metric_card("Aujourd'hui", f"{int(row['nb_communes_aujourd_hui']):,}"), unsafe_allow_html=True)
    col2.markdown(_metric_card("Hier",        f"{int(row['nb_communes_hier']):,}"),        unsafe_allow_html=True)
    col3.markdown(_metric_card("Manquantes",  f"{manquantes:,}", color=manq_color),        unsafe_allow_html=True)
    col4.markdown(_metric_card("Statut",      row["statut"],     color=statut_color),      unsafe_allow_html=True)


def _render_manquants(df_manquants):
    st.markdown("""
        <div class="section-header" style="margin-top:2rem">
            <span class="section-title">Départements manquants · dernier jour</span>
            <div class="section-line"></div>
        </div>
    """, unsafe_allow_html=True)

    manquants = df_manquants[df_manquants["code_departement"].notna()]

    if manquants.empty:
        st.markdown("""
            <div style="font-family:'DM Mono',monospace;font-size:0.75rem;color:#2DD4BF;
                        padding:1rem;border:1px solid rgba(45,212,191,0.2);background:rgba(45,212,191,0.04)">
                ✓ Aucun département manquant — couverture complète vs hier
            </div>
        """, unsafe_allow_html=True)
    else:
        df_display = manquants[["date_check", "code_departement", "statut", "statut_global"]].copy()
        df_display["date_check"] = df_display["date_check"].astype(str).str[:10]
        df_display.columns = ["Date", "Département", "Statut", "Statut global"]

        def color_statut(val):
            colors = {
                "OK":       "color:#2DD4BF",
                "WARN":     "color:#F59E0B",
                "CRITICAL": "color:#F87171",
                "MANQUANT": "color:#F87171",
            }
            return colors.get(val, "")

        st.dataframe(
            df_display.style.applymap(color_statut, subset=["Statut", "Statut global"]),
            use_container_width=True,
            hide_index=True,
        )