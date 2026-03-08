# atmo-data-platform

Pipeline de données pour l'ingestion et l'analyse quotidienne de la qualité de l'air en France, basé sur les données open data des AASQA (Associations agréées de surveillance de la qualité de l'air).

## Stack

- **Airflow** - orchestration du pipeline quotidien
- **dbt + DuckDB** - transformations et stockage analytique
- **Docker** - environnement local reproductible
- **Streamlit** - dashboard de visualisation

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

## Schéma des données

### Staging — `stg_atmo_daily`

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