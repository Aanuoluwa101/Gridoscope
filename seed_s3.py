import pandas as pd
import os
import boto3
import tempfile

df = pd.read_json('data3.jsonl', lines=True)

df['timestamp'] = pd.to_datetime(df['timestamp'])
df['year']  = df['timestamp'].dt.year
df['month'] = df['timestamp'].dt.month
df['day']   = df['timestamp'].dt.day
df['hour']  = df['timestamp'].dt.hour

s3 = boto3.client('s3')

for (year, month, day, hour), group in df.groupby(['year','month','day','hour']):
    s3_path = f"raw/meter-readings/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/part-0.parquet"
    clean = group.drop(columns=['year','month','day','hour'])
    
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        tmp_path = tmp.name

    try:
        clean.to_parquet(tmp_path, index=False)
        s3.upload_file(tmp_path, 'gridoscope-raw-dev', s3_path)
        print(f"Uploaded: {s3_path}")
    finally:
        os.remove(tmp_path)