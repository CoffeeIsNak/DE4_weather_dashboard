import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from dependencies import *  # API Key, DB 정보 등

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

# MySQL 엔진 생성
engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}')

def fetch_weather_data():
    response = requests.get(BASE_URL, params=PARAMS)
    if response.status_code == 200:
        content = response.text
        data_lines = [line for line in content.splitlines() if not line.startswith("#")]
        return data_lines
    else:
        print("API 요청 오류:", response.status_code)
        return None

def fetch_record_count():
    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM DE4_PROJECT2.Medium_term_forecast_area"))
        record_count = result.scalar()  # 레코드 개수 반환
    return record_count

def save_to_mysql(data):
    # 데이터프레임 생성
    col_name = ['REG_ID', 'TM_ST', 'TM_ED', 'REG_SP', 'REG_NAME']
    df = pd.DataFrame(columns=col_name)

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

# 데이터 수집 및 저장
data = fetch_weather_data()
if data:
    record_count = fetch_record_count()
    
    # 업데이트가 있는지 확인 (기록이 0보다 클 경우에만 데이터 저장)
    if len(data) > 0 and record_count < len(data):
        save_to_mysql(data)
        print("데이터가 MySQL에 성공적으로 저장되었습니다.")
    else:
        print("업데이트할 데이터가 없습니다.")
else:
    print("데이터를 가져오는 데 실패했습니다.")