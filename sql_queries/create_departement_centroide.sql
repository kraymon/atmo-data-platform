SELECT
    code_departement,
    round(avg(longitude), 4) as lon,
    round(avg(latitude),  4) as lat
FROM stg_atmo_daily
WHERE longitude IS NOT NULL
AND   latitude  IS NOT NULL
GROUP BY code_departement;

SELECT *
FROM stg_atmo_daily
WHERE code_departement in ('20');