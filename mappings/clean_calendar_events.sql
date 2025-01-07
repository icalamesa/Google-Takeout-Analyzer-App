CREATE OR REPLACE TABLE clean_calendar_events AS
SELECT
    'Calendar'::VARCHAR AS calendar_name,
    'Title'::VARCHAR AS title,
    STRPTIME(CAST("Start" AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS start_time,
    STRPTIME(CAST("End"   AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS end_time,
    DATEDIFF(
        'day',
        STRPTIME(CAST("End"   AS VARCHAR), '%Y-%m-%d %H:%M:%S'),
        STRPTIME(CAST("Start" AS VARCHAR), '%Y-%m-%d %H:%M:%S')
    ) AS duration,
    'Organizer'::VARCHAR AS organizer
FROM raw_calendar_events;
