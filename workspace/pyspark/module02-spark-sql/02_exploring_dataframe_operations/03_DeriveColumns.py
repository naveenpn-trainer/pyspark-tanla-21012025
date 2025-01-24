from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, count, lit, when

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Demo App").master("local").getOrCreate()
    df = spark.read.csv(path="../../resources/dataset/users/delimited_format/*",
                        header=True,
                        inferSchema=True,
                        sep="|")

    # df.withColumn("is_employeed", lit("TRUE")).show()

    (df.withColumn("salary_category",
                  when(col("salary") > 90000, "High Paid")
                  .when(col("salary") > 50000, "Medium Paid")
                  .otherwise("Low Paid")).drop("id","age")
     .show())
