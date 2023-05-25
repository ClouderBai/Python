import csv
from io import StringIO

import pandas as pd
# import pandas.io.sql as sqlio
import psycopg2
import sys

# 获取命令行参数
arguments = sys.argv

# 输出文件名
print("File name:", arguments[0])

# 输出传递的参数
for arg in arguments[1:]:
    print("Argument:", arg)
    # print(type(arg))

schema = 'cmd_owner'
tableName = arguments[1]
csv_file_path = arguments[2]
print(f'schema: {schema}, tableName: {tableName}, csv_file_path: {csv_file_path}')

if schema is None or tableName is None or csv_file_path is None:
    raise ValueError("schema table csv_file_path None")

conn = psycopg2.connect(
    "host='{}' port={} dbname='{}' user={} password={}".format('127.0.0.1', 5432, 'cmds', 'postgres', 'Win2008'))
# s_buf = StringIO()
# with open(csv_file_path, 'r', encoding='utf-8') as file:
#     reader = csv.reader(file)
#     for row in reader:
#         print(row)

# df.to_csv(s_buf, header=False, index=False)
# s_buf.seek(0)
# columns = ', '.join(['"{}"'.format(k) for k in df.columns])
cur = conn.cursor()
# sql = "COPY {} ({}) FROM STDIN WITH DELIMITER ',' CSV NULL 'NULL'".format('cmd_owner.m_sales_prd_copy1', columns)
# cur.copy_expert(sql=sql, file=s_buf)
# conn.commit()
# cur.close()
# conn.close()
cur.execute(f'TRUNCATE {schema}.{tableName};')
copy_query = f"COPY {schema}.{tableName} FROM '{csv_file_path}' DELIMITER ',' CSV HEADER"
print(copy_query)
cur.execute(copy_query)
conn.commit()
cur.close()
conn.close()

if __name__ == '__main__':
    pass
