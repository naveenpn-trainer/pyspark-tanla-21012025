from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, mean, round, split
def logic_01(col):
    return col


if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Demo App").master("local").getOrCreate()

    df = spark.read.csv(path="../../resources/dataset/employee.csv",
                        header=True,
                        inferSchema=True,
                        sep="|",
                        quote="'")
    df.show(truncate=False)
    df.printSchema()

    result_df = ((df.withColumn("skills", split(col("col_skills"),","))
                    .withColumn("previous_expected_salary", split(col("col_previous_expected_salary"),",").cast("array<int>")))
                 .drop("col_previous_expected_salary","col_skills"))

    df_01 = (result_df.withColumn("previous_salary", col("previous_expected_salary").getItem(0))
             .withColumn("expected_salary", col("previous_expected_salary").getItem(1)))


    df_01.show(truncate=False)
