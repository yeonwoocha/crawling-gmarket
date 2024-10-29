import json
import pandas as pd 

df = pd.read_json('gmarket_all.json')

print(df)

print(df.name)


food_keywords = ['쌀', '라면', '두유', '우유', '사과', '비엔나', '고기', '참치', '음료', '빵', '과일', '계란', '감자']

df['label'] = df['name'].apply(lambda x: 1 if any(keyword in x for keyword in food_keywords) else 0)

print(df)

df.to_json('gmarket_data_labeled.json', orient='records', force_ascii=False, indent=4)



