import os
import logging
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
from hdfs import InsecureClient
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
import pandas as pd
import os
import glob
from datetime import datetime # 날짜와 시간 함께 다룰때, 시간 조작, 차이 계산


def setup_logging(log_file: str = './crawling.log') -> logging.Logger:
    """로깅 설정 함수."""
    logger = logging.getLogger()

    # 이미 핸들러가 추가되어 있는지 확인
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


class Crawling:
    def __init__(self) -> None:
        # ChromeDriver 초기화
        self.logger = setup_logging()
        options = webdriver.ChromeOptions()
        #options.add_argument('--headless')  # 헤드리스 모드로 설정 (UI 없이 실행)
        options.add_argument('--disable-gpu')  # 성능 향상
        options.add_argument('--no-sandbox')  # 리눅스 환경에서 권한 문제 해결
        #service = Service(executable_path=ChromeDriverManager().install())
        self.driver = webdriver.Chrome(options=options)

    def crawl(self, url: str):
        self.driver.get(url)
        data = []
        for i in range(1, 201):
            try:
                item_selector = f"//div[@id='container']/div[2]/ul/li[{i}]"
                item_element = self.driver.find_element(By.XPATH, item_selector)

                rank = item_element.find_element(By.XPATH, ".//a/div[1]/span").text
                name = item_element.find_element(By.XPATH, ".//a/div[2]/p").text
                
                try:
                    original_price = item_element.find_element(By.CSS_SELECTOR, "div.box__price-original > span.text.text__value").text
                except:
                    original_price = 'N/A'
                try:    
                    sale_price = item_element.find_element(By.CSS_SELECTOR, "div.box__price-seller > span.text.text__value").text
                except:
                    sale_price = 'N/A'

                data.append((rank, name, original_price, sale_price))
            except Exception as e:
                self.logger.error(f"Error occurred: {e}")
        
        return data  # 수집한 데이터를 반환

    def close(self):
        self.driver.quit()

class store:
    def __init__(self, host):
        self.host = host
        self.directory_path = r'C:\Users\USER\YU\YU_python\crawling-data\crawl_data'
        self.all_file_list = glob.glob(os.path.join(self.directory_path, '*.csv'))
        self.now = now = datetime.now()
        self.year = now.strftime('%Y')
        self.month = now.strftime('%m')
        self.day = now.strftime('%d')
        self.hour = now.strftime('%H')
        self.minute = now.strftime('%M')
    
    def hadoop(self):
        # HDFS 클라이언트 초기화
        client_hdfs = InsecureClient(self.host, user='itcous')
        for file_path in self.all_file_list:

            # 파일명 추출
            filename = os.path.basename(file_path)
            filename_without_ext = os.path.splitext(filename)[0]  # 확장자 제거

            # DataFrame을 읽어서 Parquet 형식으로 변환
            df = pd.read_csv(file_path, sep=",", encoding='utf-8')
            table = pa.Table.from_pandas(df)
            
            # BytesIO 객체에 Parquet 데이터 저장
            buffer = BytesIO()
            pq.write_table(table, buffer, compression='snappy') # 
            buffer.seek(0)

            # snappy 검증
            output_dir = r'./'
            df.to_parquet(os.path.join(output_dir, f'{filename_without_ext}_1.snappy.parquet'), compression='snappy')
            
            # HDFS에 저장
            hdfs_path = f'/test/{self.year}/{self.month}/{self.day}/{self.hour}/{self.minute}/{filename_without_ext}.snappy.parquet'
            with client_hdfs.write(hdfs_path, overwrite=True) as writer:
                writer.write(buffer.getvalue())

    # def mongodb(self, host):
    #     # mongodb 클라이언트



def main():
    crawler = Crawling()
    data = crawler.crawl('https://www.gmarket.co.kr/n/best')
    # 데이터 저장 (예: JSON 파일)
    if data:
        output_dir = './crawl_data'
        os.makedirs(output_dir, exist_ok=True)
        df = pd.DataFrame(data, columns=['rank', 'name', 'original_price', 'sale_price'])
        df.to_json(os.path.join(output_dir, 'gmarket_all.json'), orient="records", force_ascii=False, indent=4)
        df.to_csv(os.path.join(output_dir, 'gmarket_all.csv'), sep=',', encoding='utf-8-sig')   # utf-8로 하면 한글깨짐
        df.to_parquet(os.path.join(output_dir, 'gmarket_all.parquet'), compression='snappy')

    crawler.close()  # 메서드 호출
    hadoop_host = 'http://10.10.20.134:9870'
    store_instance = store(hadoop_host)
    store_instance.hadoop()


if __name__ == "__main__":
    main()