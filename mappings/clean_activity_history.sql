CREATE OR REPLACE TABLE clean_activity_history AS
SELECT
    platform::VARCHAR AS platform,
    action_code::VARCHAR AS action_code,
    STRPTIME(CAST("timestamp" AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS activity_timestamp,
    link_action_name::VARCHAR AS link_action_name,
    link_action_text::VARCHAR AS link_action_text,
    channel_link::VARCHAR AS channel_link,
    channel_name::VARCHAR AS channel_name,
    link3::VARCHAR AS link3,
    link3_text::VARCHAR AS link3_text
FROM raw_activity_history
WHERE "timestamp" IS NOT NULL
  AND "timestamp" != link_action_text
  AND "timestamp" != '-1';
