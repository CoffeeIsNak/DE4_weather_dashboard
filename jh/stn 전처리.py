import os
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
from jh_dependencies import *

os.chdir(DIRECTORY)

kr_regions = gpd.read_file("법정구역_시도.geojson")
df = pd.read_csv('stn.csv', na_values = '\\N')


for i in tqdm(range(len(df))):
    latitude, longitude = df.loc[i, '위도'], df.loc[i, '경도']
    point = Point(longitude, latitude)
    
    # 좌표가 포함된 행정 구역 찾기
    matched_region = kr_regions[kr_regions.contains(point)]
    
    if not matched_region.empty:
        kor_name = matched_region.iloc[0]['CTP_KOR_NM']  # GeoJSON 파일에 ISO 코드가 포함된 열
        eng_name = matched_region.iloc[0]['CTP_ENG_NM']  # GeoJSON 파일에 ISO 코드가 포함된 열
        iso_code = matched_region.iloc[0]['CTPRVN_CD']  # GeoJSON 파일에 ISO 코드가 포함된 열
        
        df.loc[i, 'ISO 3166-2_CODE'] = iso_code
        df.loc[i, 'ISO 3166-2_KR'] = kor_name
        df.loc[i, 'ISO 3166-2_EN'] = eng_name


from sqlalchemy import create_engine
# 데이터베이스 연결 생성 (SQLite 예시)


engine = create_engine(f'mysql+pymysql://{USERNAME}:{PASSWORD}@{MYSQL_ENDPOINT}/{MYSQL_SCHEMA}')
df.to_sql(name='bronze_weather_data_STN', con=engine, if_exists='replace', index=False)
