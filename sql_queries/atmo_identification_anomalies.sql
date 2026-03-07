-- requete pour identifier les communes qui ont plusieurs sessions d'atmo
SELECT *
from mart_atmo_commune
WHERE code_insee IN (
    SELECT code_insee
    FROM mart_atmo_commune
    GROUP BY code_insee
    HAVING COUNT(*) > 1
)
ORDER BY code_insee;

-- Identification des nom de commune vide / null
SELECT code_insee, nom_commune, count(*)
from stg_atmo_daily
where nom_commune is null or nom_commune = ''
group by code_insee, nom_commune;

-- Identification d'un doublon à supprimer (meme commune, meme date, mais source différent)
SELECT *
from stg_atmo_daily
where code_insee = '243500741'
order by date_ech desc;

-- Identification des type EPCI (à priori pas de doublon excepté pour les cas identifiés précédement)
SELECT *
from stg_atmo_daily
where type_zone = 'EPCI'
order by date_ech desc;

-- check the missing communes entre le 3 et le 4 mars 2026
SELECT * FROM stg_atmo_daily s1
WHERE s1.date_ech = '2026-03-03'
AND NOT EXISTS (
    SELECT 1 FROM stg_atmo_daily s2
    WHERE s2.date_ech = '2026-03-04'
    AND s2.code_insee = s1.code_insee
);