WITH raw_neighbourhoods AS (
    SELECT * FROM {{ source('airbnb', 'raw_neighbourhoods') }}
)

SELECT
    neighbourhood

FROM raw_neighbourhoods