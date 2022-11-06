from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType


if __name__ == '__main__' :
    spark = SparkSession.builder.master("local[*]").appName("Filter Out Data").getOrCreate()

    # print(spark)

    schema_data = StructType([
        StructField("id", IntegerType()),
        StructField('name', StringType()),
        StructField("gender",StringType()),
        StructField("city", StringType()),
        StructField("salary", DoubleType())
    ])

    df =spark.read.load(r"D:\notepad++ documents\emp_data.csv",format='csv',
                        schema= schema_data)

    df.printSchema()
    df.show()


    # filter

    df.filter(df.gender == "male").select('id','name','gender').show()

    df.filter(df.gender != "male").select('id','name','gender').show()


    # salary less than 5000

    # df.filter(df.salary <5000).show()

    df.filter(df.gender.startswith("m")).show()


