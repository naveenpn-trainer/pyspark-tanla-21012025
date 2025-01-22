from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Demo App").master("local").getOrCreate()


    df = spark.read.csv(path="../../resources/dataset/users/delimited_format/*",
                        header=True,
                        inferSchema=True,
                        sep="|")


    # df.write.format("json").mode("append").save(path="../../resources/dataset/users/output")
    df.write.format("parquet").mode("append").save(path="../../resources/dataset/users/output")
    df.write.save(mode="",path="",format="p")
    