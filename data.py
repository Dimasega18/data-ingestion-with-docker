import pandas as pd
from sqlalchemy import create_engine
import time 
import os
import argparse

def connection_db(params):
    port = params.port
    password = params.password
    user = params.user
    db = params.database

    url = f"postgresql://{user}:{password}@localhost:{port}/{db}"

    return url


parser = argparse.ArgumentParser(description ='Process some integers.')

parser.add_argument('-pth','--path',dest='path',help='path untuk file parquet')
parser.add_argument('-p','--port',dest='port',help='port database')
parser.add_argument('-pass','--password',dest='password',help='password database')
parser.add_argument('-db','--database',dest='database',default='postgres',help='nama database')
parser.add_argument('-u','--user',dest='user',default='postgres',help='password database')

args = parser.parse_args()
url = connection_db(args)

df = pd.read_parquet(r"{0}".format(args.path))

df.to_csv('taxi.csv',index=False)

engine = create_engine(url)
engine.connect()

df = pd.read_csv('taxi.csv',iterator=True,chunksize=100000)

baris = 0

while True:
    try:
        df_new = next(df)
        start = time.time()

        df_new['tpep_dropoff_datetime'] = pd.to_datetime(df_new['tpep_dropoff_datetime'])
        df_new['tpep_pickup_datetime'] = pd.to_datetime(df_new['tpep_pickup_datetime'])
        df_new.to_sql('taxi_data',con=engine,index=False,if_exists='append')
        
        baris += df_new['VendorID'].count()
        end = time.time()
        waktu = end - start
        
        print(f"Data telah di insert dalam waktu {waktu} & total row {baris}")
        time.sleep(3)
        
    except Exception as e:
        print("Data telah selesai di insert",e)
        break

