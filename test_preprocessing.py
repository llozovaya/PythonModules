import json
import jsonschema
import psycopg2
from psycopg2 import sql, extras
import csv
import sys
import datetime

RAW_DATA_TABLE_NAME = 'raw_data'
METHOD_INPUT_TABLE_NAME = 'data_method_input'
METHOD_REWARD_TABLE_NAME_1 = 'data_method_reward_1'
METHOD_REWARD_TABLE_NAME_2 = 'data_method_reward_2'
DATA_ERROR_TABLE_NAME = 'data_error'

def get_data_from_DB(dbparams):
    with psycopg2.connect(**dbparams) as conn:
        cur = conn.cursor()
        cur.execute('Select * from {};'.format(RAW_DATA_TABLE_NAME))
        data = cur.fetchall()
        return data

def partition(pred, lst):
    xs, ys = [],[]
    for el in lst:
        xs.append(el) if pred(el) else ys.append(el)
    return xs, ys

def upload_to_DB(data, method, dbparams):
    with psycopg2.connect(**dbparams) as conn:
        cur = conn.cursor()
        if method == 'input':
            query = sql.SQL("INSERT INTO {} VALUES {}").format(
                    sql.Identifier(METHOD_INPUT_TABLE_NAME),
                    sql.SQL(', ').join([
                        sql.Literal((d['user'], datetime.datetime.fromtimestamp(d['ts']), json.dumps(d['context']), d['ip']))
                        for d in data])
                    )
            cur.execute(query)
        elif method == 'reward':
            reward_with_money, reward_no_money = partition(lambda row: len(row) == 4, data)
            query1 = sql.SQL("INSERT INTO {} values {}").format(
                    sql.Identifier(METHOD_REWARD_TABLE_NAME_1),
                    sql.SQL(', ').join([sql.Literal(d) for d in reward_with_money])
                    )
            query2 = sql.SQL("INSERT INTO {} values {}").format(
                    sql.Identifier(METHOD_REWARD_TABLE_NAME_2),
                    sql.SQL(', ').join([sql.Literal(d) for d in reward_no_money])
                    )
            cur.execute(query1)
            cur.execute(query2)
        else:
            query = sql.SQL("INSERT INTO {} VALUES ({}, {}, {}, {}, {})").format(
                sql.Identifier(DATA_ERROR_TABLE_NAME),
                *list(map(sql.Literal, data)))
            cur.execute(query)

schema = {
        "type": "object",
        "properties": {
            "user": {"type": "number"},
            "ts": {"type": "number"},
            "context": {"type": "object"},
            "ip": {
                "type": "string",
                "format": "ipv4"
                }
            },
        "required": ["user", "ts", "context", "ip"]
        }

def parse_data(data):
    result = data[3]
    method = data[1]
    format_ = data[4]
    try:
        if method == 'input' and format_ == 'json':
            parsed_data = [json.loads(line) for line in result.strip().split('\n')]
            # assert all([set(obj).issuperset({"user", "ts", "context", "ip"}) for obj in parsed_data]), "Unknown JSON format" # JSON objects validation
            for dat in parsed_data:
                jsonschema.validate(dat, schema, format_checker=jsonschema.FormatChecker())
            return parsed_data, method
        if method == 'reward' and format_ == 'csv':
            reader = csv.reader(result.strip().split('\n'))
            header = next(reader)
            reader = list(reader)
            assert ((header == ["user", "ts", "reward_id", "reward_money"])
                & all([len(row) in (3,4) for row in reader])), "Unknown CSV format" # csv rows length validation
            parsed_data = [(int(r[0]), datetime.datetime.fromtimestamp(float(r[1])), int(r[2])) if len(r) == 3 else
                    (int(r[0]), datetime.datetime.fromtimestamp(float(r[1])), int(r[2]), int(r[3])) for r in reader] # type convertion
            return parsed_data[1:], method
        return data[:4] + ( 'Unknown datatype',), 'error' # unknown type or format
    except Exception as e: # parse error
        return tuple(data[:4]) + ("{}: {}".format(*sys.exc_info()[:2]),), 'error'



def preprocess_data(db1params, db2params):
    all_data = get_data_from_DB(db1params)
    for data in all_data:
        parsed_data, method = parse_data(data)
        upload_to_DB(parsed_data, method, db2params)
