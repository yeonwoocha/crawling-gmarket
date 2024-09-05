from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import pandas as pd
import os


'''
pip install selenium
pip install webdriver_manager
pip install pandas
pip install pyarrow
pip install fastparquet
'''

# 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler(r"./crawling.log", mode='w', encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# WebDriver 초기화

#service = Service(ChromeDriverManager().install())  ## Chrome 드라이버 다운설치
# 크롬 옵션 설정
options = webdriver.ChromeOptions()
# options.add_argument("headless")                              ## 최대화 모드로 시작

# Selenium 초기화
service = Service(executable_path=ChromeDriverManager().install())
driver = webdriver.Chrome(options=options)

# driver = webdriver.Chrome(options=options)
driver.get('https://www.gmarket.co.kr/n/best')


# 데이터 수집
data = []
for i in range(1, 201):
    try:
        item_selector = f"//div[@id='container']/div[2]/ul/li[{i}]" # window와 같이 '\'가 포함된 경로나 패턴을 다룰때 유용하지만, f-string과 같이 변수 삽입 지원X
        item_element = driver.find_element(By.XPATH, item_selector)
    
        rank = item_element.find_element(By.XPATH, ".//a/div[1]/span").text
        name = item_element.find_element(By.XPATH, ".//a/div[2]/p").text
        
        try:
            original_price = item_element.find_element(By.CSS_SELECTOR, "div.box__price-original > span.text.text__value").text
        except:
            original_price = f'N/A'
        try:    
            sale_price = item_element.find_element(By.CSS_SELECTOR, "div.box__price-seller > span.text.text__value").text
        except:
            sale_price = 'N/A'

        data.append((rank, name, original_price, sale_price))
    except Exception as e:
        logger.error(f"Error occurred: {e}")

# JSON 파일 저장 경로 확인 및 생성
output_dir = r'./'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

print(data)

df = pd.DataFrame(data, columns=['rank', 'name', 'original_price', 'sale_price'])
df.to_json(os.path.join(output_dir, 'gmarket_all.json'), orient="records", force_ascii=False, indent=4)
df.to_csv(os.path.join(output_dir, 'gmarket_all.csv'), sep=',', encoding='utf-8-sig')   # utf-8로 하면 한글깨짐
df.to_parquet(os.path.join(output_dir, 'gmarket_all.parquet'), compression='snappy')



# 브라우저 닫기
driver.quit()