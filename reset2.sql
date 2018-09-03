DROP TABLE IF EXISTS data_method_input;
CREATE TABLE IF NOT EXISTS
data_method_input(
    "user" INT,
    ts TIMESTAMPTZ,
    context JSONB,
    ip INET
);
DROP TABLE IF EXISTS data_method_reward_1;
CREATE TABLE IF NOT EXISTS
data_method_reward_1(
    "user" INT,
    ts TIMESTAMPTZ,
    reward_id INT,
    reward_money INT
);
DROP TABLE IF EXISTS data_method_reward_2;
CREATE TABLE IF NOT EXISTS
data_method_reward_2(
    "user" INT,
    ts TIMESTAMPTZ,
    reward_id INT,
    PRIMARY KEY ("user",reward_id)
);
DROP TABLE IF EXISTS data_error;
CREATE TABLE IF NOT EXISTS
data_error(
        api_source VARCHAR,
        api_method VARCHAR,
        api_date TIMESTAMPTZ,
        result TEXT,
        error_text VARCHAR
);

