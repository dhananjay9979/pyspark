from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col


if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]').appName("Column Class").getOrCreate()

    # print(spark)
    input_data = [Row(id=1, name='Ram', address=Row(city="pune", state='MH')),
                  Row(id=2, name='Sham', address=Row(city="Nagar", state='MH'))]

    df = spark.createDataFrame(input_data)
    # df.show()

    df.select('id').show()

    df.select(df.address.city).show()
    df.select(df["address.city"]).show()
    df.select(col('address.city')).show()
    df.select(col('address.*')).show()


    # creating dataframe with select
    df1= df.select(col('address.*'))

    df1.show()
