import boto3
import pandas as pd
import os
import shutil
import numpy as np
import csv
# from pandas._config import dates
import pathlib

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)
# Bucket_Name = 'lly-cn-ibu-cmds-ods-qa-private'
# ExpectedBucketOwner = '968245374389'
Bucket_Name = 'lly-cn-ibu-cmds-ods-prd-private'
ExpectedBucketOwner = '968368533129'
BASE_PATH = 'cmds-glue/input/algnmnt'
DATE_TIME = '2023-04-20'
FULL_PATH = BASE_PATH + '/' + DATE_TIME
ifNeedS3ObjectDownload = True
archive_name = os.path.expanduser(os.path.join('~', '.aws'))
print(archive_name)

if not os.path.exists(FULL_PATH):
    os.makedirs(FULL_PATH)
else:
    files = os.listdir(FULL_PATH)
    if len(files) > 0:
        ifNeedS3ObjectDownload = False
    print('Files: ', files)

if ifNeedS3ObjectDownload:
    shutil.copy(archive_name + '\credentials-prd', archive_name + '\credentials')
    client = boto3.client('s3')
    # Download ods file
    response = client.list_objects(
        Bucket=Bucket_Name,  # Delimiter=',',EncodingType='',Marker='',MaxKeys='',RequestPayer='',
        Prefix=FULL_PATH,
        ExpectedBucketOwner=ExpectedBucketOwner
    )
    s3Objects = [f['Key'] for f in response['Contents'] if f['Size'] > 0]
    s3ObjectsDownload = [f for f in s3Objects if not os.path.exists(f)]
    for f in s3ObjectsDownload:
        print(f)
        client.download_file(Bucket_Name, f, f)

    shutil.copy(archive_name + '\credentials-qa', archive_name + '\credentials')

files = os.listdir(FULL_PATH)
s3object = FULL_PATH + '/' + files[0]
df = pd.read_csv(s3object, dtype='string')
df = df[['ALGNMNT_ID', 'CUST_ID', 'CUST_ALGNMNT_STRT_DT', 'CUST_ALGNMNT_END_DT']]
print("*" * 100 + "algnmnt")
print(df.query("ALGNMNT_ID=='CN65804' and CUST_ID=='CN-300380661HCP'"), '\n')

s3Object2 = FULL_PATH + '/' + files[1]
df2 = pd.read_csv(s3Object2, dtype='string')
df2 = df2[['ACTL_TIER', 'ALGNMNT_ID', 'CUST_ID', 'CUST_TIER_STRT_DT', 'CUST_TIER_END_DT']]
print("*" * 100 + "tier")
print(df2.query("(ALGNMNT_ID=='CN65804') and (CUST_ID=='CN-300380661HCP') and ACTL_TIER=='5'"), '\n')

combination = df.merge(df2, on=["ALGNMNT_ID", "CUST_ID"], how="inner")
print("*" * 100 + "JOIN")
print(combination.query("ALGNMNT_ID=='CN65804' and CUST_ID=='CN-300380661HCP' and ACTL_TIER=='5'"))

if __name__ == '__main__':
    pass
