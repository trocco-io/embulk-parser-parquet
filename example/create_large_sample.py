### example
## parquet
# > python example/create_large_sample.py 2800000 ./tmp/sample2800000.parquet # 81MB
# > python example/create_large_sample.py 34000000 ./tmp/sample34000000.parquet # 979MB
## csv
# > python example/create_large_sample.py 1000000 ./tmp/sample1000000.csv # 80MB
# > python example/create_large_sample.py 12000000 ./tmp/sample12000000.csv # 963MB


import pandas as pd
import random
import uuid
import time
import sys
import os
import csv

# parquet
# size = 32000000 921MB
# size = 10000000 288MB
size = int(sys.argv[1])
filepath = sys.argv[2]
_, ext = os.path.splitext(filepath)

integers = [random.randint(0, 1000000) for _ in range(size)]
floats = [random.random() for _ in range(size)]
strings = [str(uuid.uuid1()) for _ in range(size)]

def str_time_prop(start, end, time_format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formatted in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def random_date(start, end, prop):
    return str_time_prop(start, end, "%Y-%m-%dT%H:%M:%S", prop)

timestamps = [random_date("1970-01-01T00:00:00", "2020-01-01T00:00:00", random.random()) for _ in range(size)]

if ext == ".parquet":
    df = pd.DataFrame({
        "integer": integers,
        "float": floats,
        "string": strings,
        "timestamp": timestamps
    })
    print(df.info())
    df.to_parquet(filepath)
elif ext == ".csv":
    with open(filepath, "w") as f:
        writer = csv.writer(f)
        writer.writerow(["integer", "float", "string", "timestamp"])
        for i in range(size):
            writer.writerow([integers[i], floats[i], strings[i], timestamps[i]])
else:
    raise 'invalid extension. ext must be .csv or .parquet.'

