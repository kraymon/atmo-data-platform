{% macro polluant_dominant(prefix, suffix='') %}
{% set sep = '_' if suffix else '' %}
array_filter(
    ['NO2', 'O3', 'PM10', 'PM25', 'SO2'],
    x -> CASE x
        WHEN 'NO2'  THEN {{ prefix }}_no2{{ sep }}{{ suffix }}  = greatest({{ prefix }}_no2{{ sep }}{{ suffix }}, {{ prefix }}_o3{{ sep }}{{ suffix }}, {{ prefix }}_pm10{{ sep }}{{ suffix }}, {{ prefix }}_pm25{{ sep }}{{ suffix }}, {{ prefix }}_so2{{ sep }}{{ suffix }})
        WHEN 'O3'   THEN {{ prefix }}_o3{{ sep }}{{ suffix }}   = greatest({{ prefix }}_no2{{ sep }}{{ suffix }}, {{ prefix }}_o3{{ sep }}{{ suffix }}, {{ prefix }}_pm10{{ sep }}{{ suffix }}, {{ prefix }}_pm25{{ sep }}{{ suffix }}, {{ prefix }}_so2{{ sep }}{{ suffix }})
        WHEN 'PM10' THEN {{ prefix }}_pm10{{ sep }}{{ suffix }} = greatest({{ prefix }}_no2{{ sep }}{{ suffix }}, {{ prefix }}_o3{{ sep }}{{ suffix }}, {{ prefix }}_pm10{{ sep }}{{ suffix }}, {{ prefix }}_pm25{{ sep }}{{ suffix }}, {{ prefix }}_so2{{ sep }}{{ suffix }})
        WHEN 'PM25' THEN {{ prefix }}_pm25{{ sep }}{{ suffix }} = greatest({{ prefix }}_no2{{ sep }}{{ suffix }}, {{ prefix }}_o3{{ sep }}{{ suffix }}, {{ prefix }}_pm10{{ sep }}{{ suffix }}, {{ prefix }}_pm25{{ sep }}{{ suffix }}, {{ prefix }}_so2{{ sep }}{{ suffix }})
        WHEN 'SO2'  THEN {{ prefix }}_so2{{ sep }}{{ suffix }}  = greatest({{ prefix }}_no2{{ sep }}{{ suffix }}, {{ prefix }}_o3{{ sep }}{{ suffix }}, {{ prefix }}_pm10{{ sep }}{{ suffix }}, {{ prefix }}_pm25{{ sep }}{{ suffix }}, {{ prefix }}_so2{{ sep }}{{ suffix }})
    END
)
{% endmacro %}