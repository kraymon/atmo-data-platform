with source as (
    select * from read_parquet('{{ var("processed_path") }}/meteo/*.parquet')
),

cleaned as (
    select
        cast(date_ech as date)              as date_ech,
        cast(code_departement as varchar)   as code_departement,
        cast(latitude as float)             as latitude,
        cast(longitude as float)            as longitude,
        round(cast(temperature_mean as float), 1)   as temperature_mean,
        round(cast(temperature_max as float), 1)    as temperature_max,
        round(cast(temperature_min as float), 1)    as temperature_min,
        round(cast(rain_sum as float), 1)           as rain_sum,
        round(cast(precipitation_sum as float), 1)  as precipitation_sum,
        round(cast(wind_speed_max as float), 1)     as wind_speed_max,
        cast(weathercode as smallint)               as weathercode
    from source
)

select * from cleaned