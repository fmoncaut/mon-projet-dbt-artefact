with src as (

  select *
  from {{ source('raw_airbnb', 'reviews') }}

),

clean as (

  select
    -- IDs
    cast(listing_id as string) as listing_id,

    -- Selon les exports, l'id de review peut s'appeler "id" ou être absent
    cast(id as string) as review_id,

    -- Date
    safe_cast(date as date) as review_date,

    -- Reviewer
    reviewer_name,

    -- Texte
    comments

  from src

)

select * from clean
