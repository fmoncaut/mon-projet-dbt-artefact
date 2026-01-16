{{ config(materialized='table') }}

WITH date_spine AS (
  -- Génère les 5 prochaines années de dates
  SELECT 
    DATE_ADD(CURRENT_DATE(), INTERVAL day_offset DAY) as date_day
  FROM UNNEST(GENERATE_ARRAY(-365*2, 365*3)) as day_offset
)

SELECT
  date_day,
  EXTRACT(YEAR FROM date_day) as year,
  EXTRACT(MONTH FROM date_day) as month,
  FORMAT_DATE('%B', date_day) as month_name, -- Ex: January
  EXTRACT(DAYOFWEEK FROM date_day) as day_of_week, -- 1=Dimanche
  CASE WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,
  EXTRACT(QUARTER FROM date_day) as quarter
FROM date_spine