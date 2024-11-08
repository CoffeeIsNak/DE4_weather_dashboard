# Medium_term_forecast_temp_only 테이블
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pandas as pd
from dependencies import *

# API 기본 설정
BASE_URL = 'https://apihub.kma.go.kr/api/typ01/url/fct_afs_wc.php'

# 기간 설정
start_date = datetime(YEAR, MONTH, DAY) 
end_date = datetime.now()  # 오늘 날짜

# 요청 데이터 수집
all_data_lines = []

while start_date <= end_date:
    # tmfc1과 tmfc2 설정 (6개월 단위)
    tmfc1 = start_date.strftime('%Y%m%d%H%M')
    tmfc2 = (start_date + relativedelta(months=6)).strftime('%Y%m%d%H%M')

    # tmfc2가 end_date를 초과할 경우 end_date로 조정
    if datetime.strptime(tmfc2, '%Y%m%d%H%M') > end_date:
        tmfc2 = end_date.strftime('%Y%m%d%H%M')

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
        all_data_lines.extend(data_lines)  # 전체 데이터 리스트에 추가
    else:
        print("API 요청 오류:", response.status_code)

    # 시작 날짜를 6개월 후로 이동
    start_date += relativedelta(months=6)

# MySQL 엔진 생성
engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}')

# 데이터프레임으로 변환하여 MySQL에 저장
def save_to_mysql(data):
    # 데이터프레임 생성
    col_name = ['REG_ID', 'TM_FC', 'TM_EF', 'MOD', 'STN', 'C', 'MIN', 'MAX', 'MIN_L', 'MIN_H', 'MAX_L', 'MAX_H']
    df = pd.DataFrame(columns = col_name)

    df = pd.DataFrame([line.split(',')[:-1] for line in data], columns=col_name)

    df['CreateAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    df.to_sql(name='Medium_term_forecast_temp_only', con=engine, if_exists='append', index=False)

save_to_mysql(all_data_lines)
print("데이터가 MySQL에 성공적으로 저장되었습니다.")