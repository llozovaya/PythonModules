from test_download import load_from_source_to_db
from test_preprocessing import preprocess_data

db1params = {
    'host' : 'localhost',
    'dbname' : 'testdb1',
    'user' : 'python',
    'password' : 'p'
    }

db2params = {
    'host' : 'localhost',
    'dbname' : 'testdb2',
    'user' : 'python',
    'password' : 'p'
    }

server = 'http://localhost:8000/'
filename_input = 'input-2017-02-01-ok.json.gz'
filename_reward = 'reward-2017-02-01-ok.csv.gz'
filename_error_1 =  'input-2017-02-01-bad.json.gz'
filename_error_2 = 'reward-2017-02-01-bad.csv.gz'

files = [
    filename_input,
    filename_reward,
    filename_error_1,
    filename_error_2
    ]

# Example of test_download module usage
# Expected result: 4 rows in raw_data table in DB1
for filename in files:
    load_from_source_to_db(server, filename, db1params)

# Example of test_preprocessing module usage
# Expected result:
# - data from input-ok file is parsed and inserted to the table 'data_method_input'
# - data from reward-ok file is parsed and inserted to the tables 'data_method_reward_1' and 'data_method_reward_2'
# - data from input-bad and reward-bad is inserted to the table 'data_error' with error message
preprocess_data(db1params, db2params)
