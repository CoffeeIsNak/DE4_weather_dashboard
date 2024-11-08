import re
from sqlalchemy import create_engine
import datetime
import requests
import pandas as pd
from io import StringIO
from jh_dependencies import *



params = {'tm1' : '20210101',
          'tm2' : datetime.datetime.now().strftime('%Y%m%d'),
          'stn' : '0',
          'disp' : 1 ,
          'authKey' : API_AUTH_KEY,
          'help' : 0,
          }

data = requests.get(url, params = params)

# 데이터에서 필요한 행만 남기기
cleaned_data = "\n".join([line for line in data.text.splitlines() if not line.startswith("#")])

# 정규 표현식을 사용하여 가변적인 띄어쓰기를 쉼표로 변환
normalized_data = "\n".join(re.sub(r'\s{1,8}', ',', line) for line in cleaned_data.splitlines())

# 데이터프레임으로 변환
col_name = ['Date', 'STN',
'WS_AVG',
 'WR_DAY',
 'WD_MAX',
 'WS_MAX',
 'WS_MAX_TM',
 'WD_INS',
 'WS_INS',
 'WS_INS_TM',
 'TA_AVG',
 'TA_MAX',
 'TA_MAX_TM',
 'TA_MIN',
 'TA_MIN_TM',
 'TD_AVG',
 'TS_AVG',
 'TG_MIN',
 'HM_AVG',
 'HM_MIN',
 'HM_MIN_TM',
 'PV_AVG',
 'EV_S',
 'EV_L',
 'FG_DUR',
 'PA_AVG',
 'PS_AVG',
 'PS_MAX',
 'PS_MAX_TM',
 'PS_MIN',
 'PS_MIN_TM',
 'CA_TOT',
 'SS_DAY',
 'SS_DUR',
 'SS_CMB',
 'SI_DAY',
 'SI_60M_MAX',
 'SI_60M_MAX_TM',
 'RN_DAY',
 'RN_D99',
 'RN_DUR',
 'RN_60M_MAX',
 'RN_60M_MAX_TM',
 'RN_10M_MAX',
 'RN_10M_MAX_TM',
 'RN_POW_MAX',
 'RN_POW_MAX_TM',
 'SD_NEW',
 'SD_NEW_TM',
 'SD_MAX',
 'SD_MAX_TM',
 'TE_05',
 'TE_10',
 'TE_15',
 'TE_30',
 'TE_50',
 'CreateAt'
 ]

df = pd.read_csv(StringIO(normalized_data), names=col_name, sep=',')
df['CreateAt'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
df['Date'] = df['Date'].apply(lambda x : datetime.datetime.strptime(str(x), '%Y%m%d'))

 
# 데이터베이스 연결 생성 (SQLite 예시)

engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@{MYSQL_ENDPOINT}/{MYSQL_SCHEMA}')
df.to_sql(name='bronze_weather_data', con=engine, if_exists='replace', index=False)

