{{ config(materialized='table') }}

with dates as (
  select d as date_day
  from unnest(generate_date_array(
    (select min(calendar_date) from {{ ref('stg_airbnb_calendar') }}),
    (select max(calendar_date) from {{ ref('stg_airbnb_calendar') }}),
    interval 1 day
  )) as d
)

select
  date_day,
  extract(year from date_day) as year,
  extract(quarter from date_day) as quarter,
  extract(month from date_day) as month,
  format_date('%Y-%m', date_day) as year_month,
  extract(week from date_day) as week,
  extract(day from date_day) as day_of_month,
  extract(dayofweek from date_day) as day_of_week,
  format_date('%A', date_day) as day_name,
  date_trunc(date_day, month) as month_start,
  last_day(date_day, month) as month_end
from dates
