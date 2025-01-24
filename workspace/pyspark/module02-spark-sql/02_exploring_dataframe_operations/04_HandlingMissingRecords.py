from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, mean,round

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Demo App").master("local").getOrCreate()

    df = spark.read.csv(path="../../resources/dataset/employee.csv",
                        header=True,
                        inferSchema=True,
                        sep="|")
    df.show()

    # df.na.drop(subset=["col_id","col_name"]).show()
    # df.na.fill("Anonymous").na.fill(-1).show()

    mean_exp = df.select(
        round(
            mean(col("col_exp")),2)).collect()[0][0]
    print(mean_exp)
