DUCKDB_PATH = "./data/analytics/atmo.duckdb"

COULEURS_INDICE = {
    1: "#2DD4BF",
    2: "#86EFAC",
    3: "#FDE68A",
    4: "#F87171",
    5: "#B91C1C",
    6: "#6B21A8",
}

LABELS_INDICE = {
    1: "Bon",
    2: "Moyen",
    3: "Dégradé",
    4: "Mauvais",
    5: "Très mauvais",
    6: "Extrêmement mauvais",
}

COLOR_SCALE = [
    [0.0, "#2DD4BF"],
    [0.2, "#86EFAC"],
    [0.4, "#FDE68A"],
    [0.6, "#F87171"],
    [0.8, "#B91C1C"],
    [1.0, "#6B21A8"],
]

GEOJSON_URL = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements.geojson"

PLOT_BG      = "#0E1318"
PAPER_BG     = "#0E1318"
GRID_COLOR   = "#1C2530"
MUTED_COLOR  = "#4A5568"
ACCENT_COLOR = "#2DD4BF"
FONT_MONO    = "DM Mono"