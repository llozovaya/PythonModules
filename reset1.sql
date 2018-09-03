DROP TABLE IF EXISTS RAW_DATA;
CREATE TABLE IF NOT EXISTS
    raw_data(api_source VARCHAR,
        api_method VARCHAR,
        api_date TIMESTAMPTZ,
        result TEXT,
        api_param VARCHAR,
        insert_ts TIMESTAMPTZ DEFAULT 'now()'
    );
