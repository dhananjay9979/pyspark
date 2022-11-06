# import os
# import sys
#
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable



from pyspark import *
from pyspark import SparkConf
from pyspark.context import SparkContext
# sc = SparkContext.getOrCreate(SparkConf())
#
# l=[1,2,5,6]

# rdd=sparkContext
#
#


# -----------------------------------------------------------


from pyspark.sql import SparkSession
spark=SparkSession.builder.master('local[*]').appName('pyspark_demo').getOrCreate()
print("Spark Object is created")

rdd=spark.sparkContext.parallelize([1,2,4])
print(rdd.first())