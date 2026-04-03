# atmo-data-platform

Pipeline de données pour l'ingestion et l'analyse quotidienne de la qualité de l'air en France, basé sur les données open data des AASQA (Associations agréées de surveillance de la qualité de l'air).

## Stack

- **Airflow** - orchestration du pipeline quotidien
- **dbt** - transformations SQL et tests qualité
- **DuckDB** - stockage analytique local
- **Docker** - environnement local reproductible
- **Streamlit** - dashboard de visualisation
- **Partie Cloud** : Azure (VM & Blob Storage), Terraform (IaC) et Caddy (reverse proxy HTTPS)

### Principe clé

L'historique s'accumule jour après jour : un fichier CSV téléchargé puis converti en Parquet chaque jour. DuckDB est reconstruit entièrement à chaque run depuis ces fichiers.

## Source de données

**Indice ATMO quotidien par commune** - data.gouv.fr  
- URL : `https://www.data.gouv.fr/fr/datasets/indice-de-la-qualite-de-lair-quotidien-par-commune-indice-atmo/`
- Format : CSV, séparateur `,`, ~18 Mo
- Contenu : J, J+1, J+2 (~29 000 zones * 3 jours)
- Mise à jour : quotidienne
- Licence : ODbL

Le DAG filtre uniquement `date_ech == J` pour ne conserver que la donnée observée, pas les prévisions.

**Données météo** - Open-Meteo (Archive API)
- URL : `https://archive-api.open-meteo.com/v1/archive`
- Granularité : département (via centroides `departements_centroides.csv`)
- Variables : température min/max/moyenne, précipitations, vent, weathercode
- Licence : CC BY 4.0 (**usage non-commercial uniquement**)

## Schéma des données

### Staging — `stg_atmo_daily`, `stg_meteo_daily`

| Colonne | Type | Description |
|---|---|---|
| `date_ech` | date | Date de l'indice (clé primaire avec `code_insee`) |
| `code_insee` | varchar | Code INSEE commune ou EPCI |
| `code_departement` | varchar | 2 premiers caractères du code INSEE |
| `nom_commune` | varchar | Nom de la commune |
| `type_zone` | varchar | `commune` ou `EPCI` |
| `source_aasqa` | varchar | AASQA productrice de la donnée |
| `code_qual` | tinyint | Indice global 1→6 |
| `lib_qual` | varchar | Qualificatif textuel |
| `coul_qual` | varchar | Couleur hex officielle |
| `code_no2` | tinyint | Sous-indice NO2 |
| `code_o3` | tinyint | Sous-indice Ozone |
| `code_pm10` | tinyint | Sous-indice PM10 |
| `code_pm25` | tinyint | Sous-indice PM2.5 |
| `code_so2` | tinyint | Sous-indice SO2 |
| `polluants_declencheurs` | array | Polluant(s) ayant déclenché l'indice |
| `longitude` | float | Coordonnée WGS84 |
| `latitude` | float | Coordonnée WGS84 |

### Échelle des indices

| Valeur | Qualificatif |
|---|---|
| 1 | Bon |
| 2 | Moyen |
| 3 | Dégradé |
| 4 | Mauvais |
| 5 | Très mauvais |
| 6 | Extrêmement mauvais |

### Marts

| Mart | Granularité | Description |
|---|---|---|
| `mart_atmo_commune` | commune | Profil agrégé sur toute la période |
| `mart_atmo_departement_daily` | département × jour | Évolution quotidienne par département |
| `mart_atmo_daily_national` | jour | Vue nationale quotidienne |
| `mart_atmo_meteo_daily_departement` | département × jour | Croisement ATMO + météo |

### Monitoring
 
| Modèle | Description |
|---|---|
| `mon_commune_coverage` | Compare nb communes J vs J-1, statut OK/WARN/CRITICAL |
| `mon_departement_manquant` | Départements présents hier absents aujourd'hui |

## Calcul du `code_departement`
 
Le code département est résolu via deux référentiels INSEE en seed dbt, avec fallback :
 
```
1. COG 2026 (v_commune_2026)     — communes standard        → colonne DEP
2. Identifiants EPCI 2024        — EPCI                     → colonne code_departement
3. Hardcode                      — CC Baud Communauté (200096675) → 56
4. Fallback left(code_zone, 2)   — communes déléguées (COMD)
```
 
Les EPCI multi-départements (`"27,28"`) conservent le premier département via `split_part`.

## Contraintes connues

- **Pas de backfill historique** : le CSV source ne contient que 3 jours glissants (J, J+1, J+2). Chaque jour non ingéré est définitivement perdu.
- **Historique** : s'accumule uniquement via le DAG quotidien à partir de la date de démarrage du pipeline.
- **EPCI frontaliers** : certains EPCI à cheval sur deux régions 
  apparaissent avec deux lignes dans `mart_atmo_commune` 
  (une par AASQA compétente). Exemple : CA Redon Agglomération 
  (Air Breizh + Air Pays de la Loire). (**dédupliqué en gardant la ligne avec la plus mauvaise qualité d'air**)
- **Anomalies Guyane** : ~20 codes INSEE partagés entre plusieurs 
  communes dans les données Atmo Guyane. Conservés tels quels, 
  la déduplication du staging s'applique jour par jour.
- **`nom_commune` null** : ~14 communes (arrondissements de Lyon, certaines communes du Cantal) ont un libellé non renseigné à la source.
- **SQL Explorer** : exécute du SQL brut en `read_only`. Juste en local.

## Déploiement Azure (branche `cloud_azure`)

La branche `cloud_azure` contient la version déployée sur Azure.

### Infrastructure (Terraform)

- Azure Resource Group : atmo-rg (francecentral)
- VM Standard_B2ms (2 vCPU, 8 Go RAM) : Docker + Docker Compose, Airflow, Streamlit
- Storage Account : atmodataken
- Container raw/ : CSV bruts + JSON météo
- Container processed/ : Parquet

### Ce qui change par rapport au local

- **Stockage (Data Lake)** : les fichiers raw et processed sont sur **Azure Blob Storage** au lieu du filesystem local
- **DAGs** : upload/download via `azure-storage-blob` + lecture dbt via `abfs://` (fsspec)
- **Streamlit** : containerisé dans Docker, accessible publiquement via reverse proxy (Caddy)
- **mon_commune_coverage** : ce mart a été retiré dans la partie cloud pour le moment

### Dashboard public

https://atmo-dashboard.francecentral.cloudapp.azure.com