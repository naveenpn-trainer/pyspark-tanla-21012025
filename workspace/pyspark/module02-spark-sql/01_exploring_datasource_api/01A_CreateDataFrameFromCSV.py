from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Demo App").master("local").getOrCreate()

    dfr = spark.read
    print(type(dfr))
    df = dfr.csv(path="../../resources/dataset/users/csv_format/*")
    print(type(df))

    df = spark.read.csv(path="../../resources/dataset/users/delimited_format/*",
                        header=True,
                        inferSchema=True,
                        sep="|")
    df.show(n=3,truncate=False)
    df.printSchema()