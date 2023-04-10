import numpy as np
# import pandas as pd
import csv

import pandas as pd
from pandas._config import dates

with open('D:/Download/PRD_DATA/hcm_emply_2023.2.3.csv', newline='', encoding='utf8') as csvfile:
    # spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
    # for row in spamreader:
    #     print('|'.join(row))

    csvRead = pd.read_csv(csvfile, index_col=0, parse_dates=True)

    # print(csvRead.dtypes)
    # index = csvRead.index()
    # print(list(index))
    # head = csvRead.head(10)
    # i=0
    # for h in head:
    #     i = i + 1
    #     print(i, h) # first_name__v
    print(csvRead.groupby('glbl_emply_id').sum())
    # print(csvRead.head(0))
# csv = pd.read_csv('D:/Download/hcp_0001.csv', delimiter=',')
# print(csv.head(0))

if __name__ == '__main__':
    pass

# import zipfile
# print(zipfile.ZIP_DEFLATED)

# import os
# loc = os.getcwd();
# print(loc)
# with open('D:/Download/title.csv', encoding="utf8") as f:
#     print(f.read(20))
