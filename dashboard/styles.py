import streamlit as st


def inject_styles():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Syne:wght@400;600;700;800&display=swap');

        :root {
            --bg:       #080C10;
            --surface:  #0E1318;
            --border:   #1C2530;
            --accent:   #2DD4BF;
            --accent2:  #F59E0B;
            --text:     #E2E8F0;
            --muted:    #4A5568;
            --good:     #2DD4BF;
            --medium:   #86EFAC;
            --degraded: #FDE68A;
            --bad:      #F87171;
            --worst:    #B91C1C;
        }

        html, body, [data-testid="stAppViewContainer"] {
            background-color: var(--bg) !important;
            color: var(--text) !important;
            font-family: 'Syne', sans-serif !important;
        }

        [data-testid="stHeader"] { background: transparent !important; }

        [data-testid="stMainBlockContainer"] {
            padding: 2rem 3rem !important;
            max-width: 1600px !important;
        }

        /* Hero */
        .hero {
            display: flex;
            align-items: flex-end;
            justify-content: space-between;
            padding: 2.5rem 0 2rem 0;
            border-bottom: 1px solid var(--border);
            margin-bottom: 2.5rem;
        }
        .hero-title {
            font-size: 3.2rem;
            font-weight: 800;
            letter-spacing: -0.04em;
            line-height: 1;
            color: var(--text);
        }
        .hero-title span { color: var(--accent); }
        .hero-sub {
            font-family: 'DM Mono', monospace;
            font-size: 0.75rem;
            color: var(--muted);
            letter-spacing: 0.12em;
            text-transform: uppercase;
            margin-top: 0.5rem;
        }
        .live-badge {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            font-family: 'DM Mono', monospace;
            font-size: 0.7rem;
            color: var(--accent);
            letter-spacing: 0.1em;
            text-transform: uppercase;
            border: 1px solid var(--accent);
            padding: 0.4rem 0.8rem;
            border-radius: 2px;
        }
        .live-dot {
            width: 6px; height: 6px;
            border-radius: 50%;
            background: var(--accent);
            animation: pulse 2s infinite;
        }
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50%       { opacity: 0.3; }
        }

        /* KPI grid */
        .kpi-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 1px;
            background: var(--border);
            border: 1px solid var(--border);
            margin-bottom: 2.5rem;
        }
        .kpi-card {
            background: var(--surface);
            padding: 1.5rem;
            position: relative;
            overflow: hidden;
        }
        .kpi-card::before {
            content: '';
            position: absolute;
            top: 0; left: 0; right: 0;
            height: 2px;
            background: var(--accent);
            opacity: 0.6;
        }
        .kpi-label {
            font-family: 'DM Mono', monospace;
            font-size: 0.65rem;
            letter-spacing: 0.15em;
            text-transform: uppercase;
            color: var(--muted);
            margin-bottom: 0.75rem;
        }
        .kpi-value {
            font-size: 2.4rem;
            font-weight: 800;
            letter-spacing: -0.03em;
            color: var(--text);
            line-height: 1;
        }
        .kpi-sub {
            font-family: 'DM Mono', monospace;
            font-size: 0.7rem;
            color: var(--muted);
            margin-top: 0.5rem;
        }

        /* Section headers */
        .section-header {
            display: flex;
            align-items: center;
            gap: 1rem;
            margin-bottom: 1.25rem;
            margin-top: 2rem;
        }
        .section-title {
            font-size: 0.7rem;
            font-family: 'DM Mono', monospace;
            letter-spacing: 0.2em;
            text-transform: uppercase;
            color: var(--muted);
            white-space: nowrap;
        }
        .section-line {
            flex: 1;
            height: 1px;
            background: var(--border);
        }

        /* Dataframe */
        [data-testid="stDataFrame"] {
            border: 1px solid var(--border) !important;
        }

        /* Inputs */
        [data-testid="stTextInput"] input,
        [data-testid="stTextArea"] textarea {
            background: var(--surface) !important;
            border: 1px solid var(--border) !important;
            color: var(--text) !important;
            font-family: 'DM Mono', monospace !important;
            border-radius: 2px !important;
        }

        /* Buttons */
        [data-testid="stButton"] button {
            background: var(--accent) !important;
            color: var(--bg) !important;
            font-family: 'Syne', sans-serif !important;
            font-weight: 700 !important;
            border: none !important;
            border-radius: 2px !important;
            letter-spacing: 0.05em !important;
        }

        /* Tabs */
        [data-testid="stTabs"] [role="tab"] {
            font-family: 'DM Mono', monospace !important;
            font-size: 0.7rem !important;
            letter-spacing: 0.15em !important;
            text-transform: uppercase !important;
            color: var(--muted) !important;
        }
        [data-testid="stTabs"] [role="tab"][aria-selected="true"] {
            color: var(--accent) !important;
            border-bottom-color: var(--accent) !important;
        }

        /* Tags */
        .tag {
            display: inline-block;
            font-family: 'DM Mono', monospace;
            font-size: 0.65rem;
            padding: 0.2rem 0.5rem;
            border: 1px solid var(--accent);
            color: var(--accent);
            margin-right: 0.3rem;
            letter-spacing: 0.05em;
        }
        .tag-warn {
            border-color: var(--accent2);
            color: var(--accent2);
        }
        </style>
    """, unsafe_allow_html=True)