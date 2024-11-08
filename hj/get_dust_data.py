import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine
from io import StringIO
from hj_dependencies import *

url = DUST_URL

params = {'tm1' : '202305010001',
          'tm2' : '202306010000',
          'authKey' : API_AUTH_KEY,
          }

data = requests.get(url, params = params)

cleaned_data = '\n'.join([line for line in data.text.splitlines() if not line.startswith("#")])
normalized_data = StringIO(cleaned_data.replace(" ", "").replace("=", ""))
columns = [
    'TM',
    'STN_ID',
    'PM_10',
    'FLAG',
    'MQC',
    'CreateAt'
]
df = pd.read_csv(normalized_data, header=None, names=columns, sep=',')
df['TM'] = pd.to_datetime(df['TM'], format='%Y%m%d%H%M', errors='coerce')
df['CreateAt'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df.to_csv('testtt.csv', index=False)


engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@{MYSQL_ENDPOINT}/{MYSQL_SCHEMA}')
df.to_sql(name='bronze_dust_data', con=engine, if_exists='replace', index=False)
