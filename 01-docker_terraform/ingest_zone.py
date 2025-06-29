import pandas as pd
import re
import wget
from sqlalchemy import create_engine

def main():
    filelink = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    filename = re.search(r'[^/]+$', filelink).group()
    engine = create_engine("postgresql://root:root@localhost:5433/ny_taxi")
    wget.download(url = filelink, out = filename)
    zone = pd.read_csv(filename).to_sql(con = engine, name = 'zone', if_exists = 'replace')

if __name__ == "__main__":
    main()