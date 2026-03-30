WITH all_locations AS (
    SELECT DISTINCT
        borough,
        incident_zip AS zip_code
    FROM {{ ref('stg_nyc_311_dot') }}
    WHERE borough IS NOT NULL

    UNION DISTINCT

    SELECT DISTINCT
        borough_clean AS borough,
        zip AS zip_code
    FROM {{ ref('stg_nyc_restaurant_apps') }}
    WHERE borough_clean IS NOT NULL
),

location_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['borough', 'zip_code']) }} AS location_key,
        borough,
        zip_code
    FROM all_locations
)

SELECT *
FROM location_dimension