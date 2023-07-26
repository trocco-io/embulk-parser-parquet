import pandas as pd
from datetime import datetime

df = pd.DataFrame({
    "id": [1, 2, 3],
    "string": ["Tanaka", "Suzuki", "Sato"],
    "number": [3.5, None, 4.2],
    "date_string": ["1970-01-01T16:07:48", "2002-09-04T16:07:30", "2022-09-04T16:07:48"],
    "datetime": [datetime.now(), datetime(2010, 12, 8, 13, 12, 1), datetime(1970, 1, 1, 0, 1, 0)],
    "array": [["tag0", "tag1"], ["tag2"], ["tag3"]],
    "object": [{"key0":"k", "key1":"k"}, {"key0":"a"}, {"key2":"a"}],
})
print(df.info())
df.to_parquet("./example/sample.parquet")
