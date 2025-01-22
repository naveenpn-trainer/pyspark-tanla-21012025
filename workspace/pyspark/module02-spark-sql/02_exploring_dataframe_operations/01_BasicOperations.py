from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Demo App").master("local").getOrCreate()

    df = spark.read.csv(path="../../resources/dataset/users/delimited_format/*",
                        header=True,
                        inferSchema=True,
                        sep="|")
    df.show()
    result_df = df.select("id", "gen")
    result_df.show()

    # df.select("id","gen").show()
    df.select(col("id").alias("user_id"),
              col("gen")
              ).show()

    df.filter(col("gen")=="M").show()