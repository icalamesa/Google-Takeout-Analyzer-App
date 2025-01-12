CREATE OR REPLACE TABLE clean_calendar_events AS
SELECT
    'calendar'::VARCHAR AS calendar_name,
    'title'::VARCHAR AS title,
    STRPTIME(CAST("start" AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS start_time,
    STRPTIME(CAST("end"   AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS end_time,
    DATEDIFF(
        'day',
        STRPTIME(CAST("end"   AS VARCHAR), '%Y-%m-%d %H:%M:%S'),
        STRPTIME(CAST("start" AS VARCHAR), '%Y-%m-%d %H:%M:%S')
    ) AS duration,
    'organizer'::VARCHAR AS organizer
FROM raw_calendar_events;
