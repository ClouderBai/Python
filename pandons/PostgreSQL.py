import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from Common_Method import log


na = np.NaN
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)
# dialect+driver://username:password@host:port/database
engine = create_engine("postgresql://postgres:Win2008@localhost:5432/cmds")
"""
    - *dbname*: the database name
    - *database*: the database name (only as keyword argument)
    - *user*: user name used to authenticate
    - *password*: password used to authenticate
    - *host*: database host address (defaults to UNIX socket if not provided)
    - *port*: connection port number (defaults to 5432 if not provided)
"""
# connect(host='127.0.0.1', dbname='cmds', user='postgres', password='Win2008')
# df = pd.read_sql_query('SELECT \
#                             hco.hco_id as "hcoId"\
#                             ,hco.hco_name as "hcoName"\
#                             ,hco.stts_ind as "sttsInd"\
#                             ,hco.vn_entity_id as "vnEntityId"\
#                             ,hco.merged_to as "mergedTo"\
#                             ,hcoMergedTo.hco_id as "mergedToHcoId"\
#                             ,hco.clsfctn_name as "hcoType"\
#                             FROM cmd_owner.m_hco hco\
#                             LEFT JOIN cmd_owner.m_hco hcoMergedTo on hco.merged_to = hcoMergedTo.vn_entity_id ', engine, index_col='hcoId')

df_gk = pd.read_sql_query('SELECT prd_gk "groupKey", start_cycle, end_cycle, stts_ind FROM cmd_owner.m_sales_prd', engine, index_col='groupKey')
df_thc = pd.read_csv('./csvfile/THC_202304_all_49323.csv')
df_thc.columns = ['yearmonth', 'buName', 'groupKey', 'categoryId', 'categoryName', 'hcoStarId', 'hcoId', 'hcoName',
                  'status', 'status_desc', 'mergedVeevaId', 'mergedHcoName', 'commonHco']

df_merge_gk = df_thc.merge(df_gk, on='groupKey')
# log(df_thc.value_counts(), df_gk.count(), df_merge_gk.count())
# log(df_thc.loc[1])
# print(df_thc)
# 49323
# print(df_thc.merge(df, on='hcoId', how='left').)
# df_merege = df_thc.merge(df, on='hcoId', how='left')
# 49323
# print(df_merege[df_merege.isna()['mergedVeevaId']])
# print(df_thc[df_thc.duplicated(['yearmonth', 'groupKey', 'categoryId', 'hcoId'], keep=False)])

# print(df_merge_gk)
# print(df_merge_gk.query('stts_ind not in [1,0]'))
# df_merge_gk.query('stts_ind==0').loc[:, ['status']] = 'inactive'
df_invalid = df_merge_gk.query('stts_ind==0').index
log(df_merge_gk[df_invalid.values])
# df_invalid.loc[:, ['status']] = 'inactive'
# log(df_merge_gk.query('stts_ind==0'), df_invalid)
# print(df_merge_gk)

# df1 = df_gk.iloc[1:5]
# df1.index.name = 'prd_gk'
# df1.index = df1.index + '_test1'
# print(df1, df1.index)
# print(df1.to_sql('cmd_owner.m_sales_prd_test', engine, if_exists='append'))














if __name__ == '__main__':
    pass