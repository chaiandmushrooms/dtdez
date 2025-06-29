import re
import os
import wget
import time
import argparse
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from urllib.error import URLError

def connection(server):
    engine = create_engine(server)
    tries = 0
    while tries < 3:
        try:
            engine.connect().close() # test engine connection and immediately close it
            return engine
        except OperationalError:
            print("couldn't connect to postgres. trying again in 30 sec")
            tries += 1
            time.sleep(30)
    raise OperationalError("couldn't connect to the server after 3 attempts")

def download_file(filelink):
    url = filelink
    filename = re.search(r'[^/]+$', url).group()
    tries = 0
    while tries < 3:
        try:
            wget.download(
                url = url,
                out = filename
            )
            return filename
        except URLError:
            print("couldn't find the file, trying again in 30 seconds...")
            tries += 1
            time.sleep(30)

    raise URLError("couldn't find the file after 3 attempts")


def write(engine, filename):
    if os.path.exists(filename):
        yellow_jan_21 = pq.ParquetFile(filename)
    else:
        raise FileNotFoundError("couldn't find the file")
    
    total_time = time.time()
    for batch in yellow_jan_21.iter_batches(batch_size = 50000):
        batch_time = time.time()
        (batch.to_pandas()).to_sql(con = engine, name = filename.replace('.parquet', '').replace('-', '_'), if_exists = 'append', index = False)
        print(f'batch time: {time.time() - batch_time:.2f}')
    print(f'total time: {time.time() - total_time:.2f}')

def main(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    filelink = args.url
    table = args.table
    server = f'postgresql://{user}:{password}@{host}:{port}/{table}'
    engine = connection(server)
    filename = download_file(filelink)
    write(engine, filename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest parquet data to Postgres")
    parser.add_argument('--user', type=str, required=True, help='postgres username')
    parser.add_argument('--password', type=str, required=True, help='postgres password')
    parser.add_argument('--host', type=str, required=True, help='hostname')
    parser.add_argument('--port', type=str, required=True, help='port number to connect to PostgreSQL')
    parser.add_argument('--table', type=str, required=True, help='database name')
    parser.add_argument('--url', type=str, required=True, help='url to the parquet file')
    args = parser.parse_args()
    main(args)