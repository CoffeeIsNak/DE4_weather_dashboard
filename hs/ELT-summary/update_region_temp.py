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
            # 하루 이내에 생성된 레코드를 조회하는 쿼리
            select_query = f"""
                SELECT 
                    t2.REG_NAME AS Region,
                    CASE
                        WHEN t2.REG_SP = 'A' THEN 'ground'
                        WHEN t2.REG_SP = 'H' THEN 'sea'
                        WHEN t2.REG_SP = 'C' THEN 'city'
                    END AS Region_category,
                    LEFT(t1.TM_EF, 8) AS days,
                    '00' AS hours,
                    t1.MIN AS min_temp,
                    t1.MAX AS max_temp
                FROM 
                    (
                        SELECT * 
                        FROM Medium_term_forecast_temp_only
                        WHERE CreateAt >= '{one_day_ago}'
                    ) t1
                JOIN Medium_term_forecast_area t2 ON t1.REG_ID = t2.REG_ID
            """
            cursor.execute(select_query)
            results = cursor.fetchall()  # 쿼리 결과를 가져옴

            if not results:
                print('No data to update')
                return
            
            # Region_temp 테이블에 삽입할 INSERT 쿼리
            insert_query = """
                INSERT INTO Region_temp (
                    Region, Region_category, days, hours, min_temp, max_temp
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            # results를 Region_temp 테이블에 이어붙이기
            cursor.executemany(insert_query, results)
            
            # 변경 사항 커밋
            connection.commit()
    finally:
        connection.close()

# 실행
get_new_records()