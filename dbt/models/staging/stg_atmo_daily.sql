-- stg_atmo_daily lit TOUS les parquets

with source as (
    select *
    from read_parquet('{{ var("processed_path") }}/atmo/*.parquet')
),

cleaned as (
    select
        -- Identifiants
        cast(date_ech as date)   as date_ech,
        cast(code_zone as varchar) as code_insee,
        lib_zone as nom_commune,
        type_zone,
        source as source_aasqa,

        -- Indice global
        cast(code_qual as tinyint) as code_qual,
        lib_qual,
        coul_qual,

        -- Sous-indices polluants
        cast(code_no2  as tinyint) as code_no2,
        cast(code_o3   as tinyint) as code_o3,
        cast(code_pm10 as tinyint) as code_pm10,
        cast(code_pm25 as tinyint) as code_pm25,
        cast(code_so2  as tinyint) as code_so2,

        array_filter(
            ['NO2', 'O3', 'PM10', 'PM25', 'SO2'],
            x -> CASE x
                WHEN 'NO2'  THEN code_no2  = greatest(code_no2, code_o3, code_pm10, code_pm25, code_so2)
                WHEN 'O3'   THEN code_o3   = greatest(code_no2, code_o3, code_pm10, code_pm25, code_so2)
                WHEN 'PM10' THEN code_pm10 = greatest(code_no2, code_o3, code_pm10, code_pm25, code_so2)
                WHEN 'PM25' THEN code_pm25 = greatest(code_no2, code_o3, code_pm10, code_pm25, code_so2)
                WHEN 'SO2'  THEN code_so2  = greatest(code_no2, code_o3, code_pm10, code_pm25, code_so2)
            END
        ) as polluants_declencheurs,

        -- code département (2 premiers caractères du code INSEE)
        CASE
            WHEN left(cast(code_zone as varchar), 4) = '2000'
                THEN '973'                                              -- Guyane atypique
            WHEN left(cast(code_zone as varchar), 2) = '97'
                THEN left(cast(code_zone as varchar), 3)                -- 971, 972, 974, 976
            WHEN left(cast(code_zone as varchar), 2) = '24'
                THEN substring(cast(code_zone as varchar), 3, 2)        -- EPCI → chiffres 3-4
            ELSE left(cast(code_zone as varchar), 2)                    -- communes standard
        END as code_departement,

        -- Géographie
        CASE
            WHEN cast(x_wgs84 as float) BETWEEN -180 AND 180
            AND  cast(y_wgs84 as float) BETWEEN -90  AND 90
            THEN cast(x_wgs84 as float)
            ELSE NULL
        END as longitude,

        CASE
            WHEN cast(x_wgs84 as float) BETWEEN -180 AND 180
            AND  cast(y_wgs84 as float) BETWEEN -90  AND 90
            THEN cast(y_wgs84 as float)
            ELSE NULL
        END as latitude,

    from source
    where
        -- Exclure indices invalides (0=absent, 7=événement)
        code_qual not in (0, 7)
        and code_no2 not in (0, 7)
        and code_o3  not in (0, 7)
        and code_pm10 not in (0, 7)
        and code_pm25 not in (0, 7)
        and code_so2  not in (0, 7)
        -- Exclure lignes sans commune
        and code_zone is not null
),

-- supprime les doublons avec la même commune et le même code, en gardant la ligne avec la qualité d'air la plus mauvaise
deduplicated as (
    select *,
    row_number() over (
        partition by code_insee, nom_commune, date_ech
        order by code_qual desc
    ) as rn
    from cleaned
)


select * exclude (rn)  -- exclude pour ne pas exposer rn
from deduplicated
where rn = 1