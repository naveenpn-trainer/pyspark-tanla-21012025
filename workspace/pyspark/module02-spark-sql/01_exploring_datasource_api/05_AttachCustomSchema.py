from pyspark.sql import SparkSession
from pyspark.sql.types import  *
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Demo App").master("local").getOrCreate()

    CUSTOM_SCHEMA= StructType([
        StructField("id", IntegerType()),
        StructField("age", IntegerType()),
        StructField("gender", StringType()),
        StructField("designation", IntegerType()),
        StructField("salary", IntegerType())
    ])

    df = spark.read.csv(path="../../resources/dataset/users/delimited_format/*",
                        header=True,
                        schema=CUSTOM_SCHEMA,
                        sep="|")
    df.show(n=3,truncate=False)
    df.printSchema()