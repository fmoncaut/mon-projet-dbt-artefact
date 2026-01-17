{{ config(
    materialized='table'
) }}

with date_bounds as (
    select
        min(calendar_date) as min_date,
        max(calendar_date) as max_date
    from {{ ref('stg_airbnb_calendar') }}
),

dates as (
    select d as date_day
    from date_bounds,
    unnest(generate_date_array(min_date, max_date, interval 1 day)) as d
)

select
    date_day,

    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    format_date('%Y-%m', date_day) as year_month,

    extract(week from date_day) as week_of_year,
    extract(day from date_day) as day_of_month,
    extract(dayofweek from date_day) as day_of_week,

    format_date('%A', date_day) as day_name,
    format_date('%B', date_day) as month_name,

    date_trunc(date_day, month) as month_start,
    last_day(date_day, month) as month_end,

    case
        when extract(dayofweek from date_day) in (1,7) then true
        else false
    end as is_weekend

from dates
