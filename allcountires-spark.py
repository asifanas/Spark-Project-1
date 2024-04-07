from pyspark.sql import *
import pyspark
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("allcountries-spark").config("spark.sql.legacy.timeParserPolicy", "LEGACY").master(
    "local[3]").getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(
    "datasets\Allcountries.csv")
df.show()