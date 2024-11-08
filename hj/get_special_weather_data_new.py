import requests
import pandas as pd
import datetime
from sqlalchemy import create_engine
import re
from io import StringIO

url = WEATHER_URL

params = {'tmfc1' : '202101010000',
          'tmfc2' : '202102010000',
          'authKey' : API_AUTH_KEY,
          'disp' : 1,
          'help' : 0,
          }

data = requests.get(url, params = params)

cleaned_data = '\n'.join([line for line in data.text.splitlines() if not line.startswith("#")])

normalized_data = "\n".join(re.sub(r'\s{1,4}', '', line.strip('= ,')) for line in cleaned_data.splitlines())

columns = [
    'TM_FC',
    'TM_EF',
    'TM_IN',
    'STN',
    'REG_ID',
    'WRN',
    'LVL',
    'CMD',
    'GRD',
    'CNT',
    'RPT',
    'T01',
    'T02',
    'T03',
    'T04',
    'T05',
    'T06',
    'T07',
    'T08',
    'T09',
    'T10',
    'T11',
    'T12',
    'T13',
    'T14',
    'T15',
    'T16',
    'T17',
    'T18',
    'CreateAt'
]

df = pd.read_csv(StringIO(normalized_data), names=columns, sep=',')
df['TM_FC'] = df['TM_FC'].apply(lambda x : datetime.datetime.strptime(str(x), '%Y%m%d%H%M'))
df['TM_EF'] = df['TM_EF'].apply(lambda x : datetime.datetime.strptime(str(x), '%Y%m%d%H%M'))
df['TM_IN'] = df['TM_IN'].apply(lambda x : datetime.datetime.strptime(str(x), '%Y%m%d%H%M'))
df['CreateAt'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@{MYSQL_ENDPOINT}/{MYSQL_SCHEMA}')
df.to_sql(name='bronze_special_data', con=engine, if_exists='replace', index=False)

df.to_csv('testt.csv', index=False)