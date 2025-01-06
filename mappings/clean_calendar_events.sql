CREATE OR REPLACE TABLE clean_calendar_events AS
SELECT
    'Calendar' as calendar_name,
    'Title' as title,
    'Start' as start_time,
    'End' as end_time,
    'Duration' as duration,
    'Organizer' as organizer
FROM raw_calendar_events
    