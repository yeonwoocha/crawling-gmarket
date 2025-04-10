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
import yaml
import re
'''
Gmarket 
신선식품: groupCode=100000006 
    과일/야채: subGroupCode=200000042
    쌀/잡곡/견과류: subGroupCode=200000039
    축산: subGroupCode=200000041
    수산: subGroupCode=200000040
    김치/반찬: subGroupCode=200000043
가공식품: groupCode=100000005
    냉동/간편조리식품: subGroupCode=200000036
    건강/다이어트식품: subGroupCode=200000037
    과자/간식: subGroupCode=200000034
    커피/음료/생수: subGroupCode=200000038
    캔/오일/조미료: subGroupCode=200000035
생필품/육아: groupCode=100000007
생활/주방: groupCode=100001001

'''



def setup_logging(log_file: str = r'./crawling.log') -> logging.Logger:
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

class Category():
    def __init__(self, group_name, sub_group_name) -> None:
        self._logger = setup_logging()
        self._group_name = group_name
        self._sub_group_name = sub_group_name

    def store_name(self):
        '''
        gmarket sub_category 부분에 '/' 있는 항목들 존재하기 때문 폴더로 인식
        '''
        safe_group_name = re.sub(r'[<>:"/\\|?*]', '_', self._group_name)
        safe_sub_group_name = re.sub(r'[<>:"/\\|?*]', '_', self._sub_group_name)
        file_name = f'gmarket_{safe_group_name}_{safe_sub_group_name}'
        return file_name
    
    def category(self):
        with open('./Gmarket.yml', encoding='UTF-8') as f:
        # file = yaml.load(f, Loader=yaml.Loader) 
            file = yaml.full_load(f) # yaml.load 보다 보안과 신뢰성을 더 높게 유지
        
        # name_mapping을 통해 내부 키 값을 가져옵니다
        group_data = file['Gmarket'][self._group_name]

        # group_data에서 해당 sub_group을 찾습니다
        sub_group_code = None
        for sub_group in group_data.get('subGroups', []):
            if sub_group['name'] == self._sub_group_name:
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
        
        return url

# def file_name(): # 추가
    
      
class Crawling:
    def __init__(self) -> None:
        # ChromeDriver 초기화
        self._logger = setup_logging()
        options = webdriver.ChromeOptions()
        #options.add_argument('--headless')  # 헤드리스 모드로 설정 (UI 없이 실행)
        options.add_argument('--disable-gpu')  # 성능 향상
        options.add_argument('--no-sandbox')  # 리눅스 환경에서 권한 문제 해결
        #service = Service(executable_path=ChromeDriverManager().install())
        self._driver = webdriver.Chrome(options=options)

    def crawl(self, url: str):
        self._driver.get(url)
        data = []
        for i in range(1, 201):
            try:
                item_selector = f"//div[@id='container']/div[2]/ul/li[{i}]"
                item_element = self._driver.find_element(By.XPATH, item_selector)

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
                self._logger.error(f"Error occurred: {e}")
        
        return data  # 수집한 데이터를 반환

    def close(self):
        self._driver.quit()

class store:
    def __init__(self, host):
        self._host = host
        self._directory_path = r'C:\Users\USER\YU\YU_python\crawling-data\crawl_data'
        self._all_file_list = glob.glob(os.path.join(self._directory_path, '*.csv'))
        now = datetime.now()
        self._year = now.strftime('%Y')
        self._month = now.strftime('%m')
        self._day = now.strftime('%d')
        self._hour = now.strftime('%H')
        self._minute = now.strftime('%M')
    
    def hadoop(self):
        # HDFS 클라이언트 초기화
        client_hdfs = InsecureClient(self._host, user='root')
        for file_path in self._all_file_list:
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

            # # snappy 검증
            # output_dir = r'./'
            # df.to_parquet(os.path.join(output_dir, f'{filename_without_ext}_1.snappy.parquet'), compression='snappy')
            
            # HDFS에 저장
            hdfs_path = f'/gmarket/{self._year}/{self._month}/{self._day}/{self._hour}/{self._minute}/{filename_without_ext}.snappy.parquet'
            with client_hdfs.write(hdfs_path, overwrite=True) as writer:
                writer.write(buffer.getvalue())

    # def mongodb(self, host):
    #     # mongodb 클라이언트



def main():
    category = Category("신선식품", "쌀/잡곡/견과류") # 카테고리
    url = category.category()
    file_name = category.store_name()
    crawler = Crawling()
    data = crawler.crawl(url)
    # 데이터 저장 (예: JSON 파일)
    if data:
        output_dir = r'./crawl_data'
        os.makedirs(output_dir, exist_ok=True)
        df = pd.DataFrame(data, columns=['rank', 'name', 'original_price', 'sale_price'])
        df.to_json(os.path.join(output_dir, f'{file_name}.json'), orient="records", force_ascii=False, indent=4)
        df.to_csv(os.path.join(output_dir, f'{file_name}.csv'), sep=',', encoding='utf-8-sig')   # utf-8로 하면 한글깨짐
        df.to_parquet(os.path.join(output_dir, f'{file_name}.parquet'), compression='snappy')

    crawler.close()  # 메서드 호출
    hadoop_host = 'http://namenode:9098'
    store_instance = store(hadoop_host)
    store_instance.hadoop()


if __name__ == "__main__":
    main()