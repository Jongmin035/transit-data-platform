WITH vehicle_timestamps AS (
SELECT
    vehicle_id,
    is_delayed AS current_state,
    LAG(is_delayed, 1, is_delayed) OVER (PARTITION BY vehicle_id ORDER BY event_timestamp ASC) AS previous_state,
    event_timestamp AS curr_timestamp,
    LAG(event_timestamp, 1) OVER (PARTITION BY vehicle_id ORDER BY event_timestamp ASC) AS previous_timestamp
FROM
    {{ ref('stg_cta_vehicle_positions') }}
)
SELECT
    vehicle_id,
    current_state,
    previous_state,
    curr_timestamp,
    previous_timestamp,
    (curr_timestamp - previous_timestamp) AS delay_duration
FROM vehicle_timestamps
WHERE current_state != previous_state