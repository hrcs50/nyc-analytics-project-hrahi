WITH source AS (
    SELECT *
    FROM {{ ref('stg_nyc_restaurant_apps') }}
),

joined AS (
    SELECT
        -- keys
        {{ dbt_utils.generate_surrogate_key(['application_id']) }} AS application_key,

        d.date_key,
        l.location_key,
        s.seating_type_key,

        -- raw fields
        source.application_id,
        source.restaurant_name,
        source.borough,
        source.zip,

        -- measures
        CASE WHEN source.approved_for_sidewalk_seating = 'yes' THEN 1 ELSE 0 END AS is_sidewalk,
        CASE WHEN source.approved_for_roadway_seating = 'yes' THEN 1 ELSE 0 END AS is_roadway

    FROM source

    LEFT JOIN {{ ref('dim_date') }} d
        ON CAST(source._stg_loaded_at AS DATE) = d.full_date

    LEFT JOIN {{ ref('dim_location') }} l
        ON source.borough = l.borough
        AND source.zip = l.zip_code

    LEFT JOIN {{ ref('dim_seating_type') }} s
        ON source.seating_interest_sidewalk = s.seating_interest_sidewalk
)

SELECT *
FROM joined