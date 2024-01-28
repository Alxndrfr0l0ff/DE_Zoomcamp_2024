#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


df = pq.read_table('yellow_tripdata_2021-01.parquet')
trips = df.to_pandas()

conn = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

trips.tpep_pickup_datetime = pd.to_datetime(trips.tpep_pickup_datetime)
trips.tpep_dropoff_datetime = pd.to_datetime(trips.tpep_dropoff_datetime)

print(pd.io.sql.get_schema(trips, name='yellow_taxi_data', con=conn))

trips.head(0).to_sql(name='yellow_taxi_data', con=conn, if_exists='replace')



