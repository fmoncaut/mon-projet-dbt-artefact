{{ config(materialized='table') }}

with base as (
  select
    l.neighbourhood,
    d.year_month,

    f.listing_id,
    f.calendar_date,
    f.is_available,
    f.price,

    l.review_scores_rating
  from {{ ref('fact_availability_daily') }} f
  join {{ ref('dim_listings') }} l
    on f.listing_id = l.listing_id
  join {{ ref('dim_date') }} d
    on f.calendar_date = d.date_day
),

agg as (
  select
    neighbourhood,
    year_month,

    count(distinct listing_id) as active_listings,

    avg(price) as avg_price,
    approx_quantiles(price, 100)[offset(50)] as median_price,

    avg(cast(is_available as int64)) as availability_rate,
    (1 - avg(cast(is_available as int64))) as occupancy_proxy,

    avg(review_scores_rating) as avg_review_score

  from base
  where neighbourhood is not null
  group by neighbourhood, year_month
),

reviews_monthly as (
  select
    l.neighbourhood,
    format_date('%Y-%m', r.review_date) as year_month,
    count(*) as reviews_count
  from {{ ref('fact_reviews') }} r
  join {{ ref('dim_listings') }} l
    on r.listing_id = l.listing_id
  where r.review_date is not null
  group by l.neighbourhood, year_month
)

select
  a.neighbourhood,
  a.year_month,

  a.active_listings,

  a.avg_price,
  a.median_price,

  a.availability_rate,
  a.occupancy_proxy,

  a.avg_review_score,

  -- proxy financier
  (a.avg_price * a.occupancy_proxy) as revpar_proxy,

  coalesce(rm.reviews_count, 0) as reviews_count

from agg a
left join reviews_monthly rm
  on a.neighbourhood = rm.neighbourhood
 and a.year_month = rm.year_month
