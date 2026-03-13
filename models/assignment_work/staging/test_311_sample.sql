SELECT *
FROM {{ source('raw', 'source_dot_service_requests_history') }}
LIMIT 10