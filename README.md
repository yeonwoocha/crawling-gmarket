# Gmarket 크롤링 & 데이터 파이프라인 프로젝트
이 프로젝트는 Gmarket의 베스트 상품 데이터를 카테고리별로 크롤링하고, Parquet 포맷으로 변환하여 HDFS(Hadoop Distributed File System) 에 적재하는 데이터 파이프라인 자동화 작업을 목표

## 기술 스택
-  환경 : Rocky Linux, Python 3.12
-  크롤링 : Selenium
-  파이프라인 : Apache Airflow
-  데이터 처리 : Pandas, PyArrow
-  저장 포맷 : Parquet(Snappy 압축)
-  적재 : Hadoop HDFS

## 주요 기능

### 1. 크롤링 자동화

- Selenium을 활용해 Gmarket의 전체 베스트 상품 1~200위 데이터를 수집

- 카테고리 및 서브카테고리별로 URL을 동적으로 생성하여, 다양한 카테고리의 상품 정보를 분리 수집

- 수집 정보: 순위, 상품 이름, 원래 가격, 판매 가격


### 2. 파이프라인 스케줄링 (Airflow)

- Apache Airflow를 사용해 DAG(Directed Acyclic Graph)을 구성하고, 10분 주기)로 자동 실행되도록 설정

- 각 카테고리마다 독립적인 Task 세트를 생성해, 유연하게 확장 가능한 구조


### 3. 데이터 저장 및 적재
- 크롤링한 데이터를 pandas DataFrame으로 처리한 뒤, Parquet(Snappy 압축) 포맷으로 HDFS에 저장

- Snappy 압축된 Parquet 포맷으로 HDFS에 저장
저장 경로 예시: /gmarket/{group_name}/{sub_group_name}/{year}/{month}/{day}/{hour}/{minute}/{sub_group_name}.snappy.parquet


### 4. 카테고리 매핑 관리 (YAML)
   
- Gmarket.yml 파일로 그룹/서브그룹 간의 코드 매핑을 관리


## 향후 계획 (TODO)

- Spark 분석 작업 ex) 카테고리별 베스트 상품의 전체 랭킹 비율
- 환경 로컬 -> kubernetes 환경 마이그레이션 
