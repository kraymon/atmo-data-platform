-- stg_atmo_daily lit TOUS les parquets

with source as (
    select *
    from read_parquet('{{ var("processed_path") }}/atmo/*.parquet')
),

-- Référentiels
communes as (
    select
        cast(COM as varchar)              as code_zone,
        cast(DEP as varchar) as dept
    from {{ ref('v_commune_2026') }}
    where TYPECOM != 'COMD'
),

epci as (
    select
        split_part(cast(SIREN as varchar), ',', 1)            as code_zone,
        split_part(cast(code_departement as varchar), ',', 1) as dept
    from {{ ref('identifiants-epci-2024') }}
),

cleaned as (
    select
        cast(date_ech as date)     as date_ech,
        cast(s.code_zone as varchar) as code_insee,
        lib_zone                   as nom_commune,
        type_zone,
        source                     as source_aasqa,
        cast(code_qual as tinyint) as code_qual,
        lib_qual,
        coul_qual,
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

        -- Département via référentiel INSEE, fallback SIREN EPCI
        coalesce(
            c.dept,                                          -- COM → DEP direct
            e.dept,                                          -- EPCI → seed epci
            CASE cast(s.code_zone as varchar)
                WHEN '200096675' THEN '56'                   -- CC Baud Communauté hardcodé
            END,
            left(cast(s.code_zone as varchar), 2)            -- COMD fallback
        ) as code_departement,

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
        END as latitude

    from source s
    left join communes c on cast(s.code_zone as varchar) = c.code_zone
    left join epci     e on cast(s.code_zone as varchar) = e.code_zone

    where code_qual  not in (0, 7)
      and code_no2   not in (0, 7)
      and code_o3    not in (0, 7)
      and code_pm10  not in (0, 7)
      and code_pm25  not in (0, 7)
      and code_so2   not in (0, 7)
      and s.code_zone  is not null
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