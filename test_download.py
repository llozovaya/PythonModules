import psycopg2
from psycopg2 import sql
import requests
import gzip
import re
import datetime

RAW_DATA_TABLE_NAME = 'raw_data'

def get_data_from_source(url):
    r = requests.get(url, allow_redirects=False)
    if r.status_code == 200:
        return gzip.decompress(r.content).decode()
    raise Exception(r.text)

def upload_data_to_db(data, dbparams):
    with psycopg2.connect(**dbparams) as conn:
        cur = conn.cursor()
        query = sql.SQL("INSERT INTO {} values({}, {}, {}, {}, {});").format(
            sql.Identifier(RAW_DATA_TABLE_NAME), *list(map(sql.Literal, data)))
        cur.execute(query)
        conn.commit()

def load_from_source_to_db(server, filename, dbparams):
    date = re.search('\d{4}-\d{2}-\d{2}', filename)
    date = date.group(0) if date else str(datetime.date.min) # if no data in filename date = 0001-01-01
    api_method = re.search('input|reward', filename)
    api_method = api_method.group(0) if api_method else ''
    param = re.search('json|csv', filename)
    param = param.group(0) if param else ''
    data = get_data_from_source(server + filename)
    upload_data_to_db((server, api_method, date, data, param), dbparams)

