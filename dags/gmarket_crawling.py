import os
import logging
import time
import yaml
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import glob
from io import BytesIO
from hdfs import InsecureClient
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator

# DAG 정의
default_args = {'owner': 'cha',
                'start_date': days_ago(n=1)
                }

with DAG(
    dag_id='gmarket_crawling',        
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    max_active_runs=1,
    catchup=False
) as dag:
    

    def setup_logging(log_file: str = '/data/crawl_data/crawling.log') -> logging.Logger:
        """로깅 설정 함수."""
        logger = logging.getLogger('gmarket')
        # 중복 핸들러 방지
        if logger.hasHandlers():
            return logger

        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    def category(group_name, sub_group_name, **context):
        with open('/data/airflow/dags/Gmarket.yml', encoding='UTF-8') as f:
            file = yaml.full_load(f) # yaml.load 보다 보안과 신뢰성을 더 높게 유지
        
        # name_mapping을 통해 내부 키 값을 가져옵니다
        group_data = file['Gmarket'][group_name]

        # group_data에서 해당 sub_group을 찾습니다
        sub_group_code = None
        for sub_group in group_data.get('subGroups', []):
            if sub_group['name'] == sub_group_name:
                sub_group_code = sub_group['subGroupCode']
                break

        print(f'Group Code: {group_data["groupCode"]}')
        print(f'Sub Group Code: {sub_group_code}')

        group_code = group_data["groupCode"]

        # URL 설정
        if sub_group_code:
            url = f'https://www.gmarket.co.kr/n/best?groupCode={group_code}&subGroupCode={sub_group_code}'
        else:
            url = f'https://www.gmarket.co.kr/n/best?groupCode={group_code}'
        
        ti = context['ti']
        # 한 dag에 여러개의 태스크를 돌려서, xcom 변수를 다르게 설정하기 위해 고유 task_id 변수 사용
        category_task_id = ti.task_id
        ti.xcom_push(key=f'url_{category_task_id}', value=url)
        # 카테고리별 저장경로 
        ti.xcom_push(key=f'group_name_{category_task_id}', value=group_name)
        ti.xcom_push(key=f'sub_group_name_{category_task_id}', value=sub_group_name)
        


    class Crawling:
        def __init__(self) -> None:
            self._logger = setup_logging()
            options = webdriver.ChromeOptions()
            options.add_argument('--headless=chrome')   # UI 없이 백그라운드 동작
            options.add_argument('--window-size=1920,1080') # GPU 비활성화
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-infobars')
            options.add_argument('--lang=ko-KR')    # 한글 사이트 언어 설정 
            options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.7049.84 Safari/537.36')  # 크롬 브라우저처럼 위장
            
            self._driver = webdriver.Chrome(options=options)
            self._wait = WebDriverWait(self._driver, 15)
            
        def crawl(self, url: str):
            self._driver.get(url)
            try:
                self._wait.until(EC.presence_of_element_located((By.XPATH, "//div[@id='container']/div[2]/ul/li[1]")))
            except Exception as e:
                self._logger.error(f"Page didn't load properly: {e}")
                return []
            
            data = []
            # 1 ~ 200위 랭킹 
            for i in range(1, 201):
                try:
                    item_selector = f"//div[@id='container']/div[2]/ul/li[{i}]"
                    item_element = self._driver.find_element(By.XPATH, item_selector)

                    rank = item_element.find_element(By.XPATH, ".//a/div[1]/span").text # 랭킹
                    name = item_element.find_element(By.XPATH, ".//a/div[2]/p").text    # 상품 이름
                    
                    try:
                        original_price = item_element.find_element(By.CSS_SELECTOR, "div.box__price-original > span.text.text__value").text # 원래 가격
                    except:
                        original_price = 'N/A'
                    try:    
                        sale_price = item_element.find_element(By.CSS_SELECTOR, "div.box__price-seller > span.text.text__value").text   # 판매 가격격
                    except:
                        sale_price = 'N/A'

                    data.append((rank, name, original_price, sale_price))
                except Exception as e:
                    self._logger.error(f"Error occurred: {e}")
            
            self._driver.quit()
            return data  # 수집한 데이터를 반환



    def gmarket_crawl(category_task_id, **context):
        # xcom push url
        ti = context['ti']
        # 이전 태스크의 ID 받기
        url = ti.xcom_pull(key=f'url_{category_task_id}', task_ids=category_task_id)
        crawler = Crawling()
        data = crawler.crawl(url)
        # xcom push 크롤링 시점 -> hadoop 적재 디렉토리용
        now = datetime.now()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        hour = now.strftime('%H')
        minute = now.strftime('%M')
        ti = context['ti']
        ti.xcom_push(key=f'year_{category_task_id}', value=year)
        ti.xcom_push(key=f'month_{category_task_id}', value=month)
        ti.xcom_push(key=f'day_{category_task_id}', value=day)
        ti.xcom_push(key=f'hour_{category_task_id}', value=hour)
        ti.xcom_push(key=f'minute_{category_task_id}', value=minute)
        # 데이터 존재 여부에 따라 xcom push 또는 실패 처리
        if data:
            try:
                # 크롤링 한 데이터 xcom push 큰 데이터들 push 할때 주의
                ti.xcom_push(key=f'crawl_data_{category_task_id}', value=data)
                return "SUCCESS"
            except Exception as e:
                logging.error(f'크롤링 실패: {e}')
                raise
        else:
            logging.error(f'크롤링할 데이터가 없습니다.')
            raise ValueError("크롤링 결과가 없습니다.")
        
    def hadop_store(category_task_id, crawling_task_id, **context): # 2개의 태스크에서 xcom 변수 받기 위한 taxk id
        ti = context['ti']
        # gmarket_crawling 태스크 xcom 변수
        data = ti.xcom_pull(key=f'crawl_data_{category_task_id}', task_ids=crawling_task_id)
        year = ti.xcom_pull(key=f'year_{category_task_id}', task_ids=crawling_task_id)
        month = ti.xcom_pull(key=f'month_{category_task_id}', task_ids=crawling_task_id)
        day = ti.xcom_pull(key=f'day_{category_task_id}', task_ids=crawling_task_id)
        hour = ti.xcom_pull(key=f'hour_{category_task_id}', task_ids=crawling_task_id)
        minute = ti.xcom_pull(key=f'minute_{category_task_id}', task_ids=crawling_task_id)
        
        # category 태스크 xcom 변수
        group_name = ti.xcom_pull(key=f'group_name_{category_task_id}', task_ids=category_task_id)
        sub_group_name = ti.xcom_pull(key=f'sub_group_name_{category_task_id}', task_ids=category_task_id)
    
        df = pd.DataFrame(data, columns=['rank', 'name', 'original_price', 'sale_price'])
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        hdfs_host = 'http://192.168.56.114:9870'  
        hdfs_user = 'cha'
        client_hdfs = InsecureClient(hdfs_host, user=hdfs_user)
        # hadoop 저장 경로 
        hdfs_path = f'/gmarket/{group_name}/{sub_group_name}/{year}/{month}/{day}/{hour}/{minute}/{sub_group_name}.snappy.parquet'  
        with client_hdfs.write(hdfs_path, overwrite=True) as writer:
            writer.write(buffer.getvalue())

'''
fresh: # 신선식품
    groupCode: 100000006
    subGroups:
      - name: fruit_vegetable  # 과일-채소
        subGroupCode: 200000042 
      - name: rice_grains_nuts  # 쌀-잡곡-견과류
        subGroupCode: 200000039 
      - name: meat  # 육류
        subGroupCode: 200000041 
      - name: seafood  # 수산물
        subGroupCode: 200000040 
      - name: kimchi_side  # 김치-반찬
        subGroupCode: 200000043 
'''




categories = [
    ("fresh_all_url", "fresh_all_crawling", "fresh_all_store_data", "fresh", "all"),
    ("fresh_frult_url", "fresh_frult_crawling", "fresh_frult_store_data", "fresh", "fruit_vegetable"),
    ("fresh_rice_url", "fresh_rice_crawling", "fresh_rice_store_data", "fresh", "rice_grains_nuts"),
    ("fresh_meat_url", "fresh_meat_crawling", "fresh_meat_store_data", "fresh", "meat"),
    ("fresh_seafood_url", "fresh_seafood_crawling", "fresh_seafood_store_data", "fresh", "seafood"),
    ("fresh_kimchi_url", "fresh_kimchi_crawling", "fresh_kimchi_store_data", "fresh", "kimchi_side"),
    ]

start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

for xcom_id, crawl_id, store_id, group, subgroup in categories:
        
    return_xcom = PythonOperator(
        task_id=xcom_id,
        python_callable=category,
        op_args=[group, subgroup],
        dag=dag
        )
    
    crawling = PythonOperator(
        task_id=crawl_id,
        python_callable=gmarket_crawl,
        op_kwargs={'category_task_id': xcom_id},
        dag=dag
        )
        
    store = PythonOperator(
        task_id=store_id,
        python_callable=hadop_store,
        op_kwargs={
            'category_task_id': xcom_id,
            'crawling_task_id': crawl_id
        },
        dag=dag
    )
        
    # 태스크 간 의존성 연결
    start_task >> return_xcom >> crawling >> store >> end_task

    
    
    
    