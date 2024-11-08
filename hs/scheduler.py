import schedule
import time
import subprocess

# 3개의 raw data에 해당하는 테이블 업데이트
def etl_raw_data():
    subprocess.run(["python3", "ETL-raw-data/update_medium_term_forcase_area.py"])
    subprocess.run(["python3", "ETL-raw-data/update_medium_term_forecast_from_ground.py"])
    subprocess.run(["python3", "ETL-raw-data/update_medium_term_forecast_temp_only.py"])

# 2개의 요약 테이블 업데이트
def elt_direct_summary():
    subprocess.run(["python3", "ELT-raw-data/update_region_ground.py"])
    subprocess.run(["python3", "ELT-raw-data/update_region_temp.py"])

# 기상 관측소 정보 업데이트
def elt_weather_stations():
    subprocess.run(["python3", "ELT-metadata/update_temp_table.py"])
    subprocess.run(["python3", "ELT-metadata/update_weather_station_for_ground.py"])
    subprocess.run(["python3", "ELT-metadata/update_weather_station_for_temp.py"])

# 매주 월요일 오후 6시 실행되도록 설정
schedule.every().monday.at("18:00").do(etl_raw_data)
schedule.every().monday.at("18:00").do(elt_direct_summary)
schedule.every().monday.at("18:00").do(elt_weather_stations)

while True:
    schedule.run_pending()
    time.sleep(1)
