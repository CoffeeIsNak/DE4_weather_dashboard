import pymysql
import datetime
from dependencies import *

def get_new_records():
    # MySQL 연결
    connection = pymysql.connect(**db_config)
    
    try:
        with connection.cursor() as cursor:
            # 새롭게 bronze_weather_data_STN 테이블에 생성된 레코드를 조회하여 temp_table_for_temp_list를 업데이트
            select_query = f"""
            SELECT 
                DISTINCT
                '지점명',
                SUBSTRING_INDEX(SUBSTRING_INDEX(`관리관서`, '(', -1), ')', 1) AS STN_CD,
                '지점주소',
                '노장해발고도(m)',
                `ISO 3166-2_KR`
            FROM  bronze_weather_data_STN
            WHERE SUBSTRING_INDEX(SUBSTRING_INDEX(`관리관서`, '(', -1), ')', 1) NOT IN
            (
                SELECT 
                    STN_CD
                FROM temp_table_for_temp_list
            )
            """
        
            cursor.execute(select_query)
            results = cursor.fetchall()  # 쿼리 결과를 가져옴

            if not results:
                print("No new data to insert.")
                return
        
            # temp_table_for_temp_list 테이블에 삽입할 INSERT 쿼리
            insert_query = """
                INSERT INTO Region_temp (
                    지점명, STN_CD, 지점주소, `노장해발고도(m)`, `ISO 3166-2_KR`
                ) VALUES (%s, %s, %s, %s, %s)
            """
            
            # results를 temp_table_for_temp_list 테이블에 이어붙이기
            cursor.executemany(insert_query, results)
            
            # 변경 사항 커밋
            connection.commit()
    finally:
        connection.close()


# 실행
get_new_records()