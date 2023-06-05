
from time import time
from sqlalchemy import create_engine
import pandas as pd

df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)

df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

print(pd.io.sql.get_schema(df, name='yello_taxi_data', con=engine))

df_iter = pd.read_csv('yellow_tripdata_2021-01.csv',
                      iterator=True, chunksize=100000)

df = next(df_iter)

df.head(0).to_sql(name='yello_taxi_data', con=engine, if_exists='replace')


df.to_sql(name='yello_taxi_data', con=engine, if_exists='append')

while True:
    df = next(df_iter)

    start_time = time()
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df.to_sql(name='yello_taxi_data', con=engine, if_exists='append')

    end_time = time()

    print('Inserted another batch, time taken %.3f second' %
          (end_time - start_time))
