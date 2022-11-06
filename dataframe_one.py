from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark= SparkSession.builder.master("local[*]").appName("df_one").getOrCreate()

    data=[('ram','male'),('shyam','male'),('kiran','female')]

    header=['name','gender']

    rdd=spark.sparkContext.parallelize(data)


    # creating dataframe from rdd with column name

    # df=rdd.toDF(header)

    # df.show()
    # df.printSchema()

    # create dataframe

    # df1=spark.createDataFrame(data=rdd,schema=header)
    #
    # df1.show()

    # creating dataframe from csv
    # csv_df = spark.read.csv(r"D:\notepad++ documents\emp_data.csv")
    #
    # csv_df.show()


    # creating dataframe from csv with header(schema)
    csv_df = spark.read.csv(r"D:\notepad++ documents\emp_data.csv",schema='id int,name string,gender string,city string,salary int')

    # csv_df.show()
# ---------------------------------------------------------------------------------



#sum of salary
    # csv_df.agg({"salary":'sum'}).show()

    # csv_df.select('*').show()


#max salary
    # x=csv_df.agg({'salary':'max'})
    # x.show()


    # csv_df.filter(csv_df.salary ==x.show() ).show()

    csv_df.filter(csv_df.salary == csv_df.agg({'salary':'max'})).show()

