import pymysql
import datetime
from dependencies import *

def get_new_records():
    """하루 이내 생성된 레코드를 가져오고 요약 테이블을 업데이트합니다."""
    # 현재 시간으로부터 하루 전 시간 계산
    one_day_ago = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    
    # MySQL 연결
    connection = pymysql.connect(**db_config)
    
    try:
        with connection.cursor() as cursor:
            # 새롭게 bronze_weather_data_STN 테이블에 생성된 레코드를 조회하여 temp_table_for_temp_list를 업데이트
            select_query = f"""
            SELECT 
                t2.지점명,
                t2.STN_CD,
                t2.지점주소,
                t2.`노장해발고도(m)`,
                t2.`ISO 3166-2_KR`,
                t1.STN
            FROM  
            (
                SELECT
                    DISTINCT
                    STN
                FROM Medium_term_forecast_from_ground
                WHERE CreateAt >= {one_day_ago}
            ) AS t1
            LEFT JOIN
            temp_table_for_temp_list AS t2
            ON t1.STN = t2.STN_CD
            """
        
            cursor.execute(select_query)
            results = cursor.fetchall()  # 쿼리 결과를 가져옴

            if not results:
                print("No new data to insert.")
                return
        
            # temp_table_for_temp_list 테이블에 삽입할 INSERT 쿼리
            insert_query = """
                INSERT INTO weather_station_for_ground (
                    지점명, STN_CD, 지점주소, `노장해발고도(m)`, `ISO 3166-2_KR`, STN
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            # results를 temp_table_for_temp_list 테이블에 이어붙이기
            cursor.executemany(insert_query, results)
            
            # 변경 사항 커밋
            connection.commit()
    finally:
        connection.close()

# 실행
get_new_records()