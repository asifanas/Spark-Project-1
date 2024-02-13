from pyspark.sql import *

if __name__=="__main__":

    spark = SparkSession.builder.appName("spark").master("local[2]").getOrCreate()

    df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("C:\projects\pycharm\data\Fire_Department_Calls_for_Service.csv")

    df.printSchema()
