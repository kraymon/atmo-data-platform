# atmo-data-platform

Pipeline de données pour l'ingestion et l'analyse quotidienne de la qualité de l'air en France basé sur les données open data ATMO

## Stack

- **Airflow** - orchestration du pipeline quotidien
- **dbt + DuckDB** - transformations et stockage analytique
- **Docker** - environnement local reproductible

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