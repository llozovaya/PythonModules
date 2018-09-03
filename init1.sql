CREATE TABLE IF NOT EXISTS
    raw_data(api_source varchar,
        api_method varchar,
        api_date timestamptz,
        result text,
        api_param varchar,
        insert_ts timestamptz default 'now()'
    );
