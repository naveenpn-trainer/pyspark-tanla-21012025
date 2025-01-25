import sys

import spark_functions
import spark_initializer

if __name__ == '__main__':
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = spark_initializer.get_spark_session()
    user_df = spark.read.csv(path=input_path, header=True, sep="|", inferSchema=True)

    ## Business Logic
    result_df = spark_functions.group_by(user_df, "designation", "sum", "salary")
    result_df.write.mode("overwrite").csv(path=output_path, sep=",")
