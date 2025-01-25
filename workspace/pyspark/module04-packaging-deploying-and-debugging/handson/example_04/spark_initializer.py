from pyspark.sql import SparkSession

def get_spark_session():
    # return SparkSession.builder.config("spark.sql.shuffle.partitions","3").getOrCreate()
    return SparkSession.builder.getOrCreate()