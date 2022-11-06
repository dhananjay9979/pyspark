from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("test rdd").getOrCreate()

    # print(spark)

    input_rdd = spark.sparkContext.parallelize([1,2,3,4,5,67,8,])
    print(input_rdd.collect())