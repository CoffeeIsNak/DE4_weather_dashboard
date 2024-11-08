import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pandas as pd
from dependencies import *  # API Key, DB 커넥션 정보

# API URL
BASE_URL = 'https://apihub.kma.go.kr/api/typ01/url/fct_afs_wc.php'

# MySQL 엔진 생성
engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}')

def fetch_last_date():
    # 마지막 tmfc1 값을 데이터베이스에서 가져오기
    with engine.connect() as connection:
        result = connection.execute(text("SELECT MAX(TM_FC) FROM Medium_term_forecast_temp_only"))
        last_date = result.scalar()  # 가장 최근 날짜
    return last_date

def fetch_and_save_data():
    last_date = datetime.strptime(fetch_last_date()[:8], '%Y%m%d')

    # 마지막 날짜가 None일 경우 현재 날짜로 초기화
    if last_date is None:
        last_date = datetime(2021, 1, 1)  # 기본 시작 날짜

    # 현재 날짜와 다음 일요일 계산
    end_date = datetime.now()
    next_sunday = end_date + timedelta(days=(6 - end_date.weekday()))  # 다음 일요일

    # tmfc1과 tmfc2 설정
    tmfc1 = last_date.strftime('%Y%m%d%H%M')  # 마지막 저장된 날짜
    tmfc2 = next_sunday.strftime('%Y%m%d%H%M')  # 다음 일요일 날짜

    # 요청 파라미터 설정
    PARAMS = {
        'tmfc1': tmfc1,   # 시작 날짜
        'tmfc2': tmfc2,   # 종료 날짜
        'disp': 1,        # 엑셀 형식
        'authKey': API_KEY
    }

    # API 요청
    response = requests.get(BASE_URL, params=PARAMS)
    if response.status_code == 200:
        content = response.text
        # 주석(#)으로 시작하는 행 삭제
        data_lines = [line for line in content.splitlines() if not line.startswith("#")]
        if data_lines:
            save_to_mysql(data_lines)
        else:
            print("새로운 데이터가 없습니다.")
    else:
        print("API 요청 오류:", response.status_code)

def save_to_mysql(data):
    # 데이터프레임 생성
    col_name = ['REG_ID', 'TM_FC', 'TM_EF', 'MOD', 'STN', 'C', 'MIN', 'MAX', 'MIN_L', 'MIN_H', 'MAX_L', 'MAX_H']
    df = pd.DataFrame([line.split(',')[:-1] for line in data], columns=col_name)

    df['CreateAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    df.to_sql(name='Medium_term_forecast_temp_only', con=engine, if_exists='append', index=False)
    print("데이터가 MySQL에 성공적으로 저장되었습니다.")

# 실행
fetch_and_save_data()