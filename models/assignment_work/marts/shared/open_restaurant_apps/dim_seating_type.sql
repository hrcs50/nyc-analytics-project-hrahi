WITH seating_types AS (
    SELECT DISTINCT
        seating_interest_sidewalk,
        approved_for_sidewalk_seating,
        approved_for_roadway_seating
    FROM {{ ref('stg_nyc_restaurant_apps') }}
),

cleaned AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'seating_interest_sidewalk',
            'approved_for_sidewalk_seating',
            'approved_for_roadway_seating'
        ]) }} AS seating_type_key,

        seating_interest_sidewalk,
        approved_for_sidewalk_seating,
        approved_for_roadway_seating,

        CASE
            WHEN approved_for_sidewalk_seating = 'yes' THEN 'Sidewalk Seating'
            WHEN approved_for_roadway_seating = 'yes' THEN 'Roadway Seating'
            ELSE 'Unknown'
        END AS seating_category

    FROM seating_types
)

SELECT *
FROM cleaned