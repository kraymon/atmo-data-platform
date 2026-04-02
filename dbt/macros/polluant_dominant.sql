{% macro polluant_dominant(prefix, suffix='') %}
{% set sep = '_' if suffix else '' %}
{% set no2  = prefix ~ '_no2'  ~ sep ~ suffix %}
{% set o3   = prefix ~ '_o3'   ~ sep ~ suffix %}
{% set pm10 = prefix ~ '_pm10' ~ sep ~ suffix %}
{% set pm25 = prefix ~ '_pm25' ~ sep ~ suffix %}
{% set so2  = prefix ~ '_so2'  ~ sep ~ suffix %}
CASE
    WHEN greatest({{ no2 }}, {{ o3 }}, {{ pm10 }}, {{ pm25 }}, {{ so2 }}) = {{ no2 }}  THEN 'NO2'
    WHEN greatest({{ no2 }}, {{ o3 }}, {{ pm10 }}, {{ pm25 }}, {{ so2 }}) = {{ o3 }}   THEN 'O3'
    WHEN greatest({{ no2 }}, {{ o3 }}, {{ pm10 }}, {{ pm25 }}, {{ so2 }}) = {{ pm10 }} THEN 'PM10'
    WHEN greatest({{ no2 }}, {{ o3 }}, {{ pm10 }}, {{ pm25 }}, {{ so2 }}) = {{ pm25 }} THEN 'PM25'
    WHEN greatest({{ no2 }}, {{ o3 }}, {{ pm10 }}, {{ pm25 }}, {{ so2 }}) = {{ so2 }}  THEN 'SO2'
END
{% endmacro %}