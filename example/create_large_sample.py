### example
## parquet
# > python example/create_large_sample.py 300000 ./tmp/sample300000.parquet # 9.3MB
# > python example/create_large_sample.py 2800000 ./tmp/sample2800000.parquet # 81MB
# > python example/create_large_sample.py 34000000 ./tmp/sample34000000.parquet # 979MB
## csv
# > python example/create_large_sample.py 110000 ./tmp/sample110000.csv # 8.8MB
# > python example/create_large_sample.py 1000000 ./tmp/sample1000000.csv # 80MB
# > python example/create_large_sample.py 12000000 ./tmp/sample12000000.csv # 963MB


import pandas as pd
import random
import uuid
import time
import sys
import os
import csv
import json

size = int(sys.argv[1])
filepath = sys.argv[2]
_, ext = os.path.splitext(filepath)

def str_time_prop(start, end, time_format, prop):
    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))
    ptime = stime + prop * (etime - stime)
    return time.strftime(time_format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, "%Y-%m-%dT%H:%M:%S", prop)

def create_random_integers(size):
    return [random.randint(0, 1000000) for _ in range(size)]

def create_random_floats(size):
    return [random.random() for _ in range(size)]

def create_random_strings(size):
    return [str(uuid.uuid4()) for _ in range(size)]

def create_random_timestamps(size):
    return [random_date("1970-01-01T00:00:00", "2020-01-01T00:00:00", random.random()) for _ in range(size)]

if len(sys.argv) >= 4:
    columns = json.loads(sys.argv[3])
else:
    columns = { "integer" : "integer", "float" : "float", "string" : "string", "timestamp" : "timestamp" }

data = {}

for name, column_type in columns.items():
    if column_type == "integer":
        data[name] = create_random_integers(size)
    elif column_type == "float":
        data[name] = create_random_floats(size)
    elif column_type == "string":
        data[name] = create_random_strings(size)
    elif column_type == "timestamp":
        data[name] = create_random_timestamps(size)
    else:
        raise 'invalid column type'

if ext == ".parquet":
    df = pd.DataFrame(data)
    print(df.info())
    df.to_parquet(filepath)
elif ext == ".csv":
    with open(filepath, "w") as f:
        writer = csv.writer(f)
        names = data.keys()
        writer.writerow(names)
        for i in range(size):
            row = []
            for name in names:
                row.append(data[name][i])

            writer.writerow(row)
else:
    raise 'invalid extension. ext must be .csv or .parquet.'

