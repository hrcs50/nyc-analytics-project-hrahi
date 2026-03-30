WITH source AS (
    SELECT *
    FROM {{ source('raw', 'source_nyc_open_restaurant_apps') }}
),

cleaned AS (
    SELECT
        objectid AS application_id,
        globalid,
        seating_interest_sidewalk,
        restaurant_name,
        legal_business_name,
        doing_business_as_dba,
        street,
        borough,
        zip,
        business_address,
        food_service_establishment,
        approved_for_sidewalk_seating,
        approved_for_roadway_seating,
        UPPER(TRIM(borough)) AS borough_clean,
        CURRENT_TIMESTAMP() AS _stg_loaded_at
    FROM source
    WHERE objectid IS NOT NULL
)

SELECT *
FROM cleaned