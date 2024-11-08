# Medium_term_forecast_area 테이블
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dependencies import *

# 기상청 API 정보 설정
BASE_URL = 'https://apihub.kma.go.kr/api/typ01/url/fct_medm_reg.php'
PARAMS = {
    'tmfc1': '20210101',
    'tmfc2': '20241031',
    'mode': 1,
    'disp': 1,
    'help': 1,
    'authKey': API_KEY
}

def fetch_weather_data():
    response = requests.get(BASE_URL, params=PARAMS)
    if response.status_code == 200:
        content = response.text
        data_lines = [line for line in content.splitlines() if not line.startswith("#")]
        return data_lines
    else:
        print("API 요청 오류:", response.status_code)
        return None

# MySQL 엔진 생성
engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}')

# 데이터프레임으로 변환하여 MySQL에 저장
def save_to_mysql(data):
    # 데이터프레임 생성
    col_name = ['REG_ID', 'TM_ST', 'TM_ED', 'REG_SP', 'REG_NAME']
    df = pd.DataFrame(columns = col_name)

    for line in data:
        # 각 라인을 쉼표로 분리
        split_line = line.split()
        
        # 데이터가 필요한 열 수와 일치하는지 확인
        if len(split_line) == len(col_name):
            tmp_df = pd.DataFrame([split_line], columns=df.columns)
            df = pd.concat([df, tmp_df], ignore_index=True)
        else:
            print(f"Skipping line due to unexpected format: {line}")

    df['CreateAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    df.to_sql(name='Medium_term_forecast_area', con=engine, if_exists='append', index=False)

data = fetch_weather_data()
if data:
    save_to_mysql(data)
    print("데이터가 MySQL에 성공적으로 저장되었습니다.")