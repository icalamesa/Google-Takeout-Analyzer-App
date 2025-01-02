CREATE TABLE content_data (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(255) NOT NULL,
    complete_action TEXT NOT NULL,
    action_code TEXT,
    timestamp TIMESTAMP NOT NULL,
    link_action_name TEXT,
    link_action_text TEXT,
    channel_link TEXT,
    channel_name TEXT,
    link3 TEXT,
    link3_text TEXT,
    INDEX idx_platform (platform),
    INDEX idx_timestamp (timestamp)
);
