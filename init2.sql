CREATE TABLE IF NOT EXISTS
data_method_input(
    "user" int,
    ts timestamptz,
    context jsonb,
    ip inet
);
CREATE TABLE IF NOT EXISTS
data_method_reward_1(
    "user" int,
    ts timestamptz,
    reward_id int,
    reward_money int
);
CREATE TABLE IF NOT EXISTS
data_method_reward_2(
    "user" int,
    ts timestamptz,
    reward_id int,
    PRIMARY KEY ("user",reward_id)
);
CREATE TABLE IF NOT EXISTS
data_error(
        api_source varchar,
        api_method varchar,
        api_date timestamptz,
        result text,
        error_text varchar
);

