WITH raw_calendar AS (
    SELECT * FROM {{ source('airbnb', 'raw_calendar') }}
)

SELECT
    listing_id,

    CAST(date AS DATE) AS date,

    available AS is_available,

    -- Nettoyage + conversion robuste (STRING -> NUMERIC)
    SAFE_CAST(
      NULLIF(
        REGEXP_REPLACE(price, r'[^0-9.]', ''),
        ''
      ) AS NUMERIC
    ) AS price,

    SAFE_CAST(
      NULLIF(
        REGEXP_REPLACE(adjusted_price, r'[^0-9.]', ''),
        ''
      ) AS NUMERIC
    ) AS adjusted_price,

    minimum_nights,
    maximum_nights

FROM raw_calendar
WHERE date IS NOT NULL
