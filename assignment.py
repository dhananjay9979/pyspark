from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
from pyspark.sql.functions import *



if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName("with Column").getOrCreate()
#
#     # print(spark)
#
#     schema_data = StructType([
#         StructField("id",IntegerType()),
#         StructField('name',StringType()),
#         StructField("salary",DoubleType())
#     ])
#
#     df= spark.read.load(r"D:\hadoop-env\input-data\employees.csv",format='csv',
#                         schema= schema_data)
#     # df.printSchema()
#     # df.show()
#
#     # adding state column
#
#     df.withColumn('state',lit("MH")).show()
#
#
#     #adding new column by using select
#     df.select("*",lit("Maharashtra").alias ("state")).show()

# -------------------------------------------------------------------------------
# word count
    df=spark.read.load(r"C:\Users\ADMIN\Desktop\word data.txt",format="text")

    # df.show()

    df = df.withColumn('wordCount', size(split(col('value'), ' ')))

    df.show()

    # df.agg({'wordCount':'sum'}).show()


# --------------------------------------------------------------------------------
