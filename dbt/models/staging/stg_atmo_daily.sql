-- stg_atmo_daily lit TOUS les parquets

with source as (
    select *
    from read_parquet('/opt/data/processed/*.parquet')
),

cleaned as (
    select
        -- Identifiants
        cast(date_ech as date)   as date_ech,
        cast(code_zone as varchar) as code_insee,
        lib_zone                 as nom_commune,
        type_zone,
        source                   as source_aasqa,

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

        -- Géographie
        cast(x_wgs84 as float) as longitude,
        cast(y_wgs84 as float) as latitude

    from source
    where
        -- Exclure indices invalides (0=absent, 7=événement)
        code_qual not in (0, 7)
        -- Exclure lignes sans commune
        and code_zone is not null
)

select * from cleaned