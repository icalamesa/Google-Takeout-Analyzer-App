SELECT channel_name, COUNT(*) AS total_count, 
    CAST( 
        CAST('1970-01-01' AS TIMESTAMP) 
        + CAST( 
            ROUND( 
                AVG( 
                    EXTRACT(EPOCH FROM activity_timestamp) 
                    - EXTRACT(EPOCH FROM DATE_TRUNC('day', activity_timestamp)) 
                ) 
            ) AS INT 
        ) * INTERVAL '1' SECOND 
        AS TIME 
    ) AS avg_time_of_day 
FROM clean_activity_history 
WHERE platform = 'YouTube' 
  AND action_code = 'Has visto' 
GROUP BY channel_name
ORDER BY total_count DESC;