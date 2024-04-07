from pyspark.sql import *
import pyspark
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("worldCupMatches-spark").config("spark.sql.legacy.timeParserPolicy", "LEGACY").master(
    "local[3]").getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(
    "datasets\WorldCupMatches.csv")
df.show()

# 1	How many matches were played in the year 1930?
numberOfMatches = df.where(col('Year') == 1930).count()
print('total number of matches in year 1930:-', numberOfMatches)

# 2	What is the average attendance across all matches in every year?
everyYear = df.groupby('Year').agg(round(avg('Attendance'), 2).alias('avg_attentance')).orderBy(col('Year'))
everyYear.show()

# 3	Which stadium hosted the most matches?
mostStadium = df.groupby('Stadium').agg(count('*').alias('total_number')).orderBy(col('total_number').desc())
mostnumber = mostStadium.where(col('total_number').isNotNull()) \
        .select('Stadium', 'total_number').limit(1).show()
#



