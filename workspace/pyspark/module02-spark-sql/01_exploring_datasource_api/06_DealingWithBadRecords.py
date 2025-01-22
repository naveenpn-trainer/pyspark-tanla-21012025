from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Streaming Demo").master("local").getOrCreate()

    df = spark.read.option("columnNameOfCorruptRecord","bad_record").json(path="../../resources/dataset/access_logs.json",
                         mode="PERMISSIVE")
    df.show()