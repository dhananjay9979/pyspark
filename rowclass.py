from pyspark.sql import SparkSession
from pyspark.sql import Row

if __name__ =="__main__":
    spark = SparkSession.builder.master('local[*]').appName('Rowclass').getOrCreate()

    input_data = [Row(id=1,name='Ram',address=Row(city="pune",state='MH')),
                  Row(id=2,name='Sham',address=Row(city="Nagar",state='MH'))]

    df = spark.createDataFrame(input_data)
    df.show()
    df.printSchema()