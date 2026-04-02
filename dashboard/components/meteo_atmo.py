import streamlit as st
import plotly.graph_objects as go
from config import (
    PLOT_BG, PAPER_BG, GRID_COLOR, MUTED_COLOR, ACCENT_COLOR, FONT_MONO
)

def render_meteo_atmo(data: dict):
    df = data["meteo_atmo"]

    if df.empty:
        st.info("Aucune donnée disponible.")
        return

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df["temperature_max"],
        y=df["indice_moyen"],
        mode="markers+text",
        text=df["date_ech"].astype(str).str[5:],  # MM-DD
        textposition="top center",
        textfont=dict(size=9, color=MUTED_COLOR),
        marker=dict(color=ACCENT_COLOR, size=8, opacity=0.8),
        hovertemplate="Temp. max : %{x}°C<br>Indice : %{y}<extra></extra>",
    ))

    fig.update_layout(
        plot_bgcolor=PLOT_BG,
        paper_bgcolor=PAPER_BG,
        font=dict(family=FONT_MONO, color="#E2E8F0", size=11),
        xaxis=dict(title="Température max moyenne (°C)", gridcolor=GRID_COLOR),
        yaxis=dict(title="Indice qualité air moyen", gridcolor=GRID_COLOR),
        margin=dict(l=40, r=20, t=20, b=40),
        height=400,
    )

    st.plotly_chart(fig, use_container_width=True)