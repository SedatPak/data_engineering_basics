import pandas as pd
import sqlalchemy

df = pd.read_csv("yellow_tripdata_2021-01.csv", nrows = 100)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

engine = sqlalchemy.create_engine("postgresql://root:root@localhost:5432/ny_taxi")

pd.io.sql.get_schema(df, name = 'yellow_taxi_data', con = engine)


df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)

df = next(df_iter)
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df_iter.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

for chunk in df_iter:
    chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
    chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)

    chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')