SELECT
    unique_key,
    created_date,
    closed_date,
    agency,
    complaint_type,
    descriptor,
    borough,
    incident_zip,
    street_name,
    status
FROM {{ source('raw', 'source_dot_service_requests_history') }}