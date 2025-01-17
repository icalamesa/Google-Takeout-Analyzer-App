CREATE OR REPLACE TABLE clean_chrome_history AS
WITH extracted AS (
    SELECT json_extract("Browser History", '$') AS js
    FROM raw_chrome_history
),
unnested AS (
    SELECT 
        json_extract_path_text(value, 'favicon_url') AS favicon_url,
        json_extract_path_text(value, 'page_transition_qualifier') AS page_transition_qualifier,
        json_extract_path_text(value, 'title') AS title,
        json_extract_path_text(value, 'url') AS url,
        json_extract_path_text(value, 'time_usec') AS time_usec,
        json_extract_path_text(value, 'client_id') AS client_id
    FROM extracted,
         LATERAL UNNEST(CAST(js AS JSON[])) AS t(value)
),
transformations AS (
    SELECT 
           TIMESTAMP 'epoch' + (CAST(time_usec AS BIGINT) // 1000000) * INTERVAL '1 second' AS datetime_value,
           regexp_extract(url, '^(?:https?://)?(?:www\\.)?([^/]+)', 1) AS domain,
           --regexp_extract(url, '^(?:https?://)?(?:www\\.)?[^/]+(/.*)$', 1) AS path,
           favicon_url,
           page_transition_qualifier,
           title,
           url
    FROM unnested
)
SELECT * FROM transformations;
