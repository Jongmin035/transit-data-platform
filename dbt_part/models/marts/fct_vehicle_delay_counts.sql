SELECT
    vehicle_id,
    COUNT(*) AS recovered_from_delay_count
FROM {{ ref('int_vehicle_delay_events') }}
WHERE current_state = False AND previous_state = True
GROUP BY vehicle_id
ORDER BY vehicle_id