WITH delay_duration_category AS (
    SELECT
        (CASE WHEN delay_duration < INTERVAL '5 minutes' THEN 'Short'
            WHEN delay_duration < INTERVAL '1 hour' THEN 'Medium'
            ELSE 'Long' END) AS delay_duration_category
    FROM {{ ref('int_vehicle_delay_events') }}
    WHERE current_state = False and previous_state = True
)
SELECT
    delay_duration_category,
    COUNT(*) AS duration_category_count
FROM delay_duration_category
GROUP BY delay_duration_category
ORDER BY
        CASE delay_duration_category
            WHEN 'Short' THEN 1
            WHEN 'Medium' THEN 2
            WHEN 'Long' THEN 3
        END