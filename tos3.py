import pandas as pd
import os

df = pd.read_json('data.jsonl', lines=True)

# parse timestamp so you can partition by it
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['year']  = df['timestamp'].dt.year
df['month'] = df['timestamp'].dt.month
df['day']   = df['timestamp'].dt.day
df['hour']  = df['timestamp'].dt.hour


# for (year, month, day, hour), group in df.groupby(['year','month','day','hour']):
#     path = f"raw/meter-readings/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
#     filename = f"{path}part-0.parquet"
#     group.drop(columns=['year','month','day','hour']).to_parquet(filename, index=False)
#     print(f"Written: {filename}")



for (year, month, day, hour), group in df.groupby(['year','month','day','hour']):
    path = f"raw/meter-readings/year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/"
    filename = f"{path}part-0.parquet"
    
    # create the directory if it doesn't exist
    os.makedirs(path, exist_ok=True)
    
    group.drop(columns=['year','month','day','hour']).to_parquet(filename, index=False)
    print(f"Written: {filename}")