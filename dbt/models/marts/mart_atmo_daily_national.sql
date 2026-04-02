-- depends_on: {{ ref('stg_atmo_daily') }}


with base as (
    select * from {{ ref('stg_atmo_daily') }}
),


aggregated as (
    select
        date_ech,
        count(*) as nb_communes,
        round(avg(code_qual), 2) as indice_moyen_national,
        count(*) filter (where code_qual = 1) as nb_bon,
        count(*) filter (where code_qual >= 4) as nb_mauvais_ou_pire,
        round(100.0 * count(*) filter (where polluants_declencheurs = 'NO2')  / count(*), 1) as pct_no2_declencheur,
        round(100.0 * count(*) filter (where polluants_declencheurs = 'O3')   / count(*), 1) as pct_o3_declencheur,
        round(100.0 * count(*) filter (where polluants_declencheurs = 'PM10') / count(*), 1) as pct_pm10_declencheur,
        round(100.0 * count(*) filter (where polluants_declencheurs = 'PM25') / count(*), 1) as pct_pm25_declencheur,
        round(100.0 * count(*) filter (where polluants_declencheurs = 'SO2')  / count(*), 1) as pct_so2_declencheur
    from base
    group by date_ech
),

with_dominant as (
    select
        *,
        {{ polluant_dominant('pct', 'declencheur') }} as polluants_dominants_national
    from aggregated
)

select 
    date_ech, nb_communes, 
    indice_moyen_national, 
    nb_bon,nb_mauvais_ou_pire, 
    polluants_dominants_national,
    pct_no2_declencheur,
    pct_o3_declencheur,
    pct_pm10_declencheur,
    pct_pm25_declencheur,
    pct_so2_declencheur 
from with_dominant

order by date_ech desc