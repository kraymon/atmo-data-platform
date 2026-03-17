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
 
counts as (
    select
        (select count(*) from aujourd_hui)       as nb_aujourd_hui,
        (select count(*) from hier)              as nb_hier,
        (select count(*) from communes_manquantes) as nb_manquantes
),

final as (
    select
        dates.date_aujourd_hui                   as date_check,
        dates.date_hier                          as date_reference,
        c.nb_aujourd_hui                         as nb_communes_aujourd_hui,
        c.nb_hier                                as nb_communes_hier,
        c.nb_manquantes                          as nb_communes_manquantes,
        case
            when c.nb_manquantes = 0
                then 'OK'
            when c.nb_manquantes > c.nb_hier * 0.20
                then 'CRITICAL'
            else 'WARN'
        end as statut
    from dates
    cross join counts c
)
 
select * from final
 