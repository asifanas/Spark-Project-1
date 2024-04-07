from pyspark.sql import *
import pyspark
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("allcountries-spark").config("spark.sql.legacy.timeParserPolicy", "LEGACY").master(
    "local[3]").getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load(
    "datasets\Allcountries.csv")
df.show()

# 1	What are the top 5 countries with the highest population density?
popDensity = df.select('Country') \
        .orderBy(col('Density').desc()) \
        .limit(5)
popDensity.show()

# 2	Which country has the largest land area?
largestLand = df.select('Country') \
        .orderBy(col('LandArea').desc()) \
        .limit(1)
largestLand.show()

# 3	What is the average GDP per capita among all countries?
gdpAmong = df.select('Country', round((col('GDP')/col('Population')), 2).alias('per_capita'))
gdpAmong.show()

# 4	Which country has the highest CO2 emissions per capita?
coAmong = df.select('Country', round((col('CO2')/col('Population')), 2).alias('per_capita'))
coAmong.show()

# 5	How many countries have a Rural population percentage above 50%?
rural = df.where(col('Rural') > 50) \
        .count()
print('country population > 50:-', rural)

# 6	What is the average life expectancy across all countries?
avgLife = df.agg(round(avg('LifeExpectancy'), 2).alias('life_expentancy'))
avgLife.show()

# 7	Which country has the highest Pump Price for gasoline?
highPump = df.select('Country').orderBy(col('PumpPrice').desc()).limit(1)
highPump.show()

# 8	What is the total military expenditure of all countries combined?
totalMil = df.agg(round(sum('Military'), 2).alias('military_exp'))
totalMil.show()

# 9	How many countries have a Health expenditure greater than 10% of GDP?
