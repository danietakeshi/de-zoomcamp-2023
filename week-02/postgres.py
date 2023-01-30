#!/usr/bin/env python3

import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine
from prefect import flow, task
import os

@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, db, table_name, csv_url):
    csv_name = 'output.csv'

    if csv_url.endswith('.csv.gz'):
        os.system(f'wget {csv_url} -O {csv_name}.gz')
        os.system(f'gzip -d {csv_name}.gz')
    else:
        os.system(f'wget {csv_url} -O {csv_name}')

    df = pd.read_csv(csv_name, nrows=100)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()

    pd.io.sql.get_schema(df, table_name, con=engine)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print('Inserted another chunk..., took %.3f second' % (t_end - t_start))

@flow(name="Ingest Flow")
def main_flow():
    user = "user@domain.com"
    password = "root"
    host = "192.168.15.46"
    port = "5432"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    ingest_data(user, password, host, port, db, table_name, csv_url)


if __name__ == '__main__':
    main_flow()
