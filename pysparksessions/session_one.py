from pyspark import SparkConf
from pyspark import SparkContext

if __name__ == '__main__':
    print('hello')
    sparkconf=SparkConf().setAppName("spark context init").setMaster("Local[*]")

    sc = SparkContext(conf=sparkconf)
    print(sc)