CREATE OR REPLACE TABLE clean_activity_history AS
SELECT
    platform::VARCHAR as platform,
    action_code::VARCHAR as action_code,
    "timestamp"::TIMESTAMP as activity_timestamp,
    link_action_name::VARCHAR as link_action_name,
    link_action_text::VARCHAR as link_action_text,
    channel_link::VARCHAR as channel_link,
    channel_name::VARCHAR as channel_name,
    link3::VARCHAR as link3,
    link3_text::VARCHAR as link3_text
FROM raw_activity_history;
