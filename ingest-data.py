#!/usr/bin/env python
# coding: utf-8
import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    os.system(f"wget {url}")

    chunk_size = 100_000
    parquet_file = pq.ParquetFile('yellow_tripdata_2021-01.parquet')
    conn = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    for batch in parquet_file.iter_batches(batch_size=chunk_size):
    #for i in range(0, parquet_file.num_row_groups, chunk_size):
        #table = parquet_file.read_row_group(i, i + chunk_size)
        chunk_df = pa.Table.from_batches([batch]).\
        to_pandas(split_blocks=True, self_destruct=True)
        #chunk_df = table.to_pandas(split_blocks=True, self_destruct=True)
        chunk_df.tpep_pickup_datetime = pd.to_datetime(chunk_df.tpep_pickup_datetime)
        chunk_df.tpep_dropoff_datetime = pd.to_datetime(chunk_df.tpep_dropoff_datetime)
        chunk_df.to_sql(name=f'{table_name}', con=conn, if_exists='replace')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest NY Taxi Data into Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres where we will write the results to')
    parser.add_argument('--url', help='url of the data file')

    args = parser.parse_args()
    main(args)

