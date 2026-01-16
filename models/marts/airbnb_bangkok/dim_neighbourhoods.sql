{{ config(materialized='table') }}

WITH stg_neighbourhoods AS (
    SELECT * FROM {{ ref('stg_neighbourhoods') }}
)

SELECT
    neighbourhood

FROM stg_neighbourhoods