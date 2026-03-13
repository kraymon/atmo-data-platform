-- depends_on: {{ ref('stg_atmo_daily') }}

with dates as (
    select
        max(date_ech)                    as date_aujourd_hui,
        max(date_ech) - interval '1 day' as date_hier
    from {{ ref('stg_atmo_daily') }}
),
 
aujourd_hui as (
    select code_insee
    from {{ ref('stg_atmo_daily') }}
    cross join dates
    where date_ech = dates.date_aujourd_hui
),
 
hier as (
    select code_insee
    from {{ ref('stg_atmo_daily') }}
    cross join dates
    where date_ech = dates.date_hier
),
 
communes_manquantes as (
    select
        h.code_insee,
        'MANQUANT' as statut
    from hier h
    left join aujourd_hui a using (code_insee)
    where a.code_insee is null
),
 
final as (
    select
        dates.date_aujourd_hui                          as date_check,
        dates.date_hier                                 as date_reference,
        (select count(*) from aujourd_hui)              as nb_communes_aujourd_hui,
        (select count(*) from hier)                     as nb_communes_hier,
        (select count(*) from communes_manquantes)      as nb_communes_manquantes,
        case
            when (select count(*) from communes_manquantes) = 0
                then 'OK'
            when (select count(*) from communes_manquantes) > (select count(*) * 0.20 from hier)
                then 'CRITICAL'
            else 'WARN'
        end as statut
    from dates
)
 
select * from final
 