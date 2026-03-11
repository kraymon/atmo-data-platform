import streamlit as st
import plotly.graph_objects as go
from config import PLOT_BG, GRID_COLOR, MUTED_COLOR, ACCENT_COLOR, FONT_MONO


def render_tendance(data: dict):
    df_nat = data["national"].copy()
    df_nat["date_ech"] = df_nat["date_ech"].astype(str).str[:10]

    col_a, col_b = st.columns([2, 1])

    with col_a:
        _render_courbe_indice(df_nat)
        _render_barres_bon_mauvais(df_nat)

    with col_b:
        _render_tableau_historique(df_nat)


def _render_courbe_indice(df_nat):
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_nat["date_ech"],
        y=df_nat["indice_moyen_national"],
        fill="tozeroy",
        fillcolor="rgba(45,212,191,0.08)",
        line=dict(color=ACCENT_COLOR, width=2.5),
        mode="lines+markers",
        marker=dict(size=6, color=ACCENT_COLOR, symbol="circle"),
        name="Indice moyen national",
        hovertemplate="<b>%{x}</b><br>Indice : %{y:.2f}<extra></extra>",
    ))

    fig.add_hline(
        y=3,
        line_dash="dot",
        line_color="#FDE68A",
        line_width=1,
        annotation_text="Seuil dégradé",
        annotation_font=dict(color="#FDE68A", size=10, family=FONT_MONO),
    )

    fig.update_layout(
        paper_bgcolor=PLOT_BG,
        plot_bgcolor=PLOT_BG,
        font=dict(family=FONT_MONO, color=MUTED_COLOR, size=10),
        height=320,
        margin=dict(l=10, r=10, t=30, b=10),
        xaxis=dict(
            showgrid=False,
            tickfont=dict(color=MUTED_COLOR, family=FONT_MONO, size=10),
            tickangle=0,
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=GRID_COLOR,
            range=[1, 6],
            tickvals=[1, 2, 3, 4, 5, 6],
            tickfont=dict(color=MUTED_COLOR, family=FONT_MONO, size=10),
        ),
        showlegend=False,
        title=dict(
            text="ÉVOLUTION INDICE MOYEN NATIONAL",
            font=dict(family=FONT_MONO, size=10, color=MUTED_COLOR),
            x=0,
        ),
    )

    st.plotly_chart(fig, use_container_width=True)


def _render_barres_bon_mauvais(df_nat):
    max_val = int(df_nat[["nb_bon", "nb_mauvais_ou_pire"]].max().max())
    step    = max(1, max_val // 4)
    tickvals = list(range(0, max_val + step, step))

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_nat["date_ech"],
        y=df_nat["nb_bon"],
        name="Bon",
        marker_color=ACCENT_COLOR,
        hovertemplate="<b>%{x}</b><br>Communes bonnes : %{y}<extra></extra>",
    ))

    fig.add_trace(go.Bar(
        x=df_nat["date_ech"],
        y=df_nat["nb_mauvais_ou_pire"],
        name="Mauvais+",
        marker_color="#F87171",
        hovertemplate="<b>%{x}</b><br>Communes mauvaises : %{y}<extra></extra>",
    ))

    fig.update_layout(
        paper_bgcolor=PLOT_BG,
        plot_bgcolor=PLOT_BG,
        font=dict(family=FONT_MONO, color=MUTED_COLOR, size=10),
        height=220,
        margin=dict(l=10, r=10, t=30, b=10),
        barmode="group",
        xaxis=dict(
            showgrid=False,
            tickfont=dict(color=MUTED_COLOR, size=10),
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=GRID_COLOR,
            tickfont=dict(color=MUTED_COLOR, size=10),
            tickvals=tickvals,
            ticktext=[str(v) for v in tickvals],
            zeroline=True,
            zerolinecolor=GRID_COLOR,
        ),
        legend=dict(font=dict(color=MUTED_COLOR, size=9), bgcolor="rgba(0,0,0,0)"),
        title=dict(
            text="COMMUNES BONNES vs MAUVAISES",
            font=dict(family=FONT_MONO, size=10, color=MUTED_COLOR),
            x=0,
        ),
    )

    st.plotly_chart(fig, use_container_width=True)


def _render_tableau_historique(df_nat):
    st.markdown("""
        <div class="section-header">
            <span class="section-title">Historique</span>
            <div class="section-line"></div>
        </div>
    """, unsafe_allow_html=True)

    df_display = df_nat[["date_ech", "indice_moyen_national", "nb_communes"]].copy()
    df_display.columns = ["Date", "Indice", "Communes"]

    st.dataframe(
        df_display.style.background_gradient(
            subset=["Indice"],
            cmap="RdYlGn_r",
            vmin=1, vmax=6,
        ),
        use_container_width=True,
        height=560,
        hide_index=True,
    )