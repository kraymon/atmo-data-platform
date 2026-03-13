-- depends_on: {{ ref('mart_atmo_daily_departement') }}

with dates as (
    select
        max(date_ech)                          as date_aujourd_hui,
        max(date_ech) - interval '1 day'       as date_hier
    from {{ ref('mart_atmo_daily_departement') }}
),

hier as (
    select distinct d.code_departement
    from {{ ref('mart_atmo_daily_departement') }} d
    cross join dates
    where d.date_ech = dates.date_hier
),

aujourd_hui as (
    select distinct d.code_departement
    from {{ ref('mart_atmo_daily_departement') }} d
    cross join dates
    where d.date_ech = dates.date_aujourd_hui
),

manquants as (
    select
        h.code_departement,
        'MANQUANT' as statut
    from hier h
    left join aujourd_hui a using (code_departement)
    where a.code_departement is null
),

final as (
    select
        dates.date_aujourd_hui  as date_check,
        dates.date_hier         as date_reference,
        m.code_departement,
        m.statut,
        (select count(*) from manquants) as nb_departements_manquants,
        case
            when (select count(*) from manquants) = 0  then 'OK'
            when (select count(*) from manquants) <= 10 then 'WARN'
            else 'CRITICAL'
        end as statut_global
    from manquants m
    cross join dates

    union all

    -- Ligne synthèse si aucun manquant (pour toujours avoir une ligne à afficher)
    select
        dates.date_aujourd_hui,
        dates.date_hier,
        null                    as code_departement,
        'OK'                    as statut,
        0                       as nb_departements_manquants,
        'OK'                    as statut_global
    from dates
    where (select count(*) from manquants) = 0
)

select * from final