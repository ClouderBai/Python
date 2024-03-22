import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 使用环境变量
import os

value = os.getenv('MY_VARIABLE')
print(value)  # 输出：my_value

database = os.environ["DATABASE_NAME"]
user = os.environ["DATABASE_USERNAME"]
pwd = os.environ["DATABASE_PASSWORD"]
host = os.environ["DATABASE_HOST"]
port = os.environ["DATABASE_PORT"]


# def lambda_handler():
conn = psycopg2.connect(database=database, user=user, password=pwd, host=host, port=port)
print("Opened database successfully")
cur = conn.cursor()
cur.execute()
data = cur.fetchall()
print('data: ', len(data))
print('description: ', len(cur.description))
print('columns: ', [x.name for x in cur.description])
df = pd.DataFrame(data)
df.columns = [x.name for x in cur.description]
print('df: ', df.head())
print('df id: ', df['id'])
