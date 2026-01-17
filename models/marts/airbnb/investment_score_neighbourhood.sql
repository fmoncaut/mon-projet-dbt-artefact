{{ config(materialized='table') }}

with base as (
  select
    neighbourhood,

    avg(revpar_proxy) as avg_revpar,
    avg(occupancy_proxy) as avg_occupancy,
    avg(avg_review_score) as avg_review_score,
    avg(active_listings) as avg_active_listings

  from {{ ref('kpi_neighbourhood_monthly') }}
  group by neighbourhood
),

norm as (
  select
    neighbourhood,

    avg_revpar,
    avg_occupancy,
    avg_review_score,
    avg_active_listings,

    -- normalisations min-max
    (avg_revpar - min(avg_revpar) over()) 
      / nullif(max(avg_revpar) over() - min(avg_revpar) over(), 0) as revpar_norm,

    (avg_occupancy - min(avg_occupancy) over()) 
      / nullif(max(avg_occupancy) over() - min(avg_occupancy) over(), 0) as occupancy_norm,

    (avg_review_score - min(avg_review_score) over()) 
      / nullif(max(avg_review_score) over() - min(avg_review_score) over(), 0) as review_norm,

    -- concurrence inversée (moins = mieux)
    1 - (
      (avg_active_listings - min(avg_active_listings) over()) 
      / nullif(max(avg_active_listings) over() - min(avg_active_listings) over(), 0)
    ) as competition_norm

  from base
)

select
  neighbourhood,

  avg_revpar,
  avg_occupancy,
  avg_review_score,
  avg_active_listings,

  -- score composite pondéré
  (
      0.35 * revpar_norm
    + 0.25 * occupancy_norm
    + 0.25 * review_norm
    + 0.15 * competition_norm
  ) as investment_score

from norm
order by investment_score desc
