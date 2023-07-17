import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql import SparkSession


def main(spark):
    # df = pd.read_csv('D:/Downloads/6files/m_cstmr_seller.csv')
    df = spark.read.csv('D:/Downloads/6files/m_cstmr_seller.csv')
    print(df.count())
    df1 = df
    df2 = df1.dropna()
    print(df2.count())

    df2.write.mode()


if __name__ == "__main__":
    main(SparkSession.builder.getOrCreate())
