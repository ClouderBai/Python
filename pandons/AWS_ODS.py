import numpy as np
import csv
import boto3
import pandas as pd
# from pandas._config import dates
import os
import pathlib

# pd.set_option('max_columns', 10)
# pd.set_option("max_seq_item", None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)
Bucket_Name = 'lly-cn-ibu-cmds-ods-qa-private'
ExpectedBucketOwner = '968245374389'
DownloadDir = 'D:/Download/'
DATE_TIME = '2022-03-11'
if not os.path.exists('cmds-glue/input/algnmnt/' + DATE_TIME):
    os.makedirs('cmds-glue/input/algnmnt/' + DATE_TIME)
# client = boto3.client('s3')
# # Download ods file
# response = client.list_objects(
#     Bucket=Bucket_Name,# Delimiter=',',EncodingType='',Marker='',MaxKeys='',RequestPayer='',
#     Prefix='cmds-glue/input/algnmnt/' + DATE_TIME + '/',
#     ExpectedBucketOwner=ExpectedBucketOwner
# )
# s3Objects = [f['Key'] for f in response['Contents'] if f['Size'] > 0]
# s3ObjectsDownload = [f for f in s3Objects if not os.path.exists(f)]
# for f in s3ObjectsDownload:
#     print(f)
#     client.download_file(Bucket_Name, f, f)

s3Objects = ['cmds-glue/input/algnmnt/2022-03-11/i_ads_cust_algnmnt.csv',
             'cmds-glue/input/algnmnt/2022-03-11/i_ads_cust_algnmnt_tier_vw.csv']
s3object = s3Objects[0]
df = pd.read_csv(s3object, dtype='string')
# print(df.columns)
df = df[['ALGNMNT_ID', 'CUST_ID', 'CUST_ALGNMNT_STRT_DT', 'CUST_ALGNMNT_END_DT']]
print("*" * 100 + "algnmnt")
print(df.query("ALGNMNT_ID=='CN65804' and CUST_ID=='CN-300380661HCP'"))
print("-" * 100 + "")

s3Object2 = s3Objects[1]
df2 = pd.read_csv(s3Object2, dtype='string')
# print(df2.columns)
df2 = df2[['ACTL_TIER', 'ALGNMNT_ID', 'CUST_ID', 'CUST_TIER_STRT_DT', 'CUST_TIER_END_DT']]
print("*" * 100 + "tier")
print(df2.query("(ALGNMNT_ID=='CN65804') and (CUST_ID=='CN-300380661HCP') and ACTL_TIER=='5'"))
print("-" * 100 + "")

combination = df.merge(df2, on=["ALGNMNT_ID", "CUST_ID"], how="inner")
print("*" * 100 + "JOIN")
print(combination.query("ALGNMNT_ID=='CN65804' and CUST_ID=='CN-300380661HCP' and ACTL_TIER=='5'"))
print("*" * 100 + "")

if __name__ == '__main__':
    pass
