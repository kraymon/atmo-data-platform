import streamlit as st


def render_monitoring(data: dict):
    df_manquants = data["monitoring"]
    _render_status_banner(df_manquants)
    _render_manquants(df_manquants)


def _statut_badge(statut: str) -> str:
    colors = {"OK": "#2DD4BF", "WARN": "#F59E0B", "CRITICAL": "#F87171"}
    color = colors.get(statut, "#4A5568")
    return (
        f'<span style="font-family:\'DM Mono\',monospace;font-size:0.7rem;font-weight:600;'
        f'color:{color};border:1px solid {color};padding:0.15rem 0.5rem;border-radius:2px;">'
        f'{statut}</span>'
    )


def _render_status_banner(df_manquants):
    statut_manquants = df_manquants["statut_global"].iloc[0] if not df_manquants.empty else "—"
    nb_manquants     = int(df_manquants["nb_departements_manquants"].iloc[0]) if not df_manquants.empty else 0

    banner_colors = {
        "OK":       ("rgba(45,212,191,0.05)",  "#2DD4BF"),
        "WARN":     ("rgba(245,158,11,0.05)",  "#F59E0B"),
        "CRITICAL": ("rgba(248,113,113,0.05)", "#F87171"),
    }
    bg, border = banner_colors.get(statut_manquants, ("rgba(74,85,104,0.05)", "#4A5568"))

    st.markdown(f"""
        <div style="background:{bg};border:1px solid {border};padding:1.25rem 1.5rem;
                    margin-bottom:2rem;display:flex;align-items:center;justify-content:space-between;">
            <div style="display:flex;align-items:center;gap:1rem">
                {_statut_badge(statut_manquants)}
                <span style="font-family:'DM Mono',monospace;font-size:0.75rem;color:#E2E8F0">
                    {nb_manquants} département(s) manquant(s) vs hier
                </span>
            </div>
        </div>
    """, unsafe_allow_html=True)


def _render_manquants(df_manquants):
    st.markdown("""
        <div class="section-header">
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