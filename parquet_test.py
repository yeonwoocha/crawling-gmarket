import logging
import pandas as pd
import os
from hdfs import InsecureClient
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO


parquet_file  = r'C:\Users\USER\YU\YU_python\crawling-data\test\country_wise_latest.snappy.parquet'

pq_file = pq.ParquetFile(parquet_file)

df_parquet = pd.read_parquet(parquet_file)
df_parquet1 = pd.read_parquet(parquet_file, engine='pyarrow')
df_parquet2= pd.read_parquet(parquet_file, engine='fastparquet')




print(pq_file.metadata)
print(df_parquet)
print()
print()
print(df_parquet1)
print()
print()
print(df_parquet2)
