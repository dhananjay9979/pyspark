from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,StructField

if __name__ == '__main__':
    spark= SparkSession.builder.master('local[*]').appName("empty dataframe").getOrCreate()
    # empty rdd
    header=['name','gender']
    empty_rdd =spark.sparkContext.emptyRDD()


# creating customised schema
    schema_data= StructType([
        StructField("name",StringType()),
        StructField("gender",StringType())
    ])

    # print(empty_rdd.getNumPartitions())
    #
    # df = spark.createDataFrame(empty_rdd,schema_data)
    #
    # df.show()
    #
    # df.printSchema()

    # ---------------------------------
    df = empty_rdd.toDF(schema_data)

    df.show()
