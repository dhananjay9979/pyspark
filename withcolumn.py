from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType


if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName("with Column").getOrCreate()

    # print(spark)

    schema_data = StructType([
        StructField("id",IntegerType()),
        StructField('name',StringType()),
        StructField("salary",DoubleType())
    ])

    df= spark.read.load(r"D:\hadoop-env\input-data\employees.csv",format='csv',
                        schema= schema_data)
    df.printSchema()
    df.show()



    # changing data type of column
    df1= df.withColumn("salary",col("salary").cast("Integer"))
    # df.show()

    df.withColumn("salary",col("salary") * 100).show()

    # rename column
    df.withColumnRenamed("name","first_name").printSchema()

#     drop column
    df.drop("city").printSchema()

# assignment
# add a column state value(MH)
