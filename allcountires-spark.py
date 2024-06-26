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
gdpPer = df.select('Country').where(col('Health') > 10).limit(1).show()

# 12	How many countries have a Hunger score greater than 20?
hungerScore = df.select('Country') \
        .where(col('Hunger') > 20).show()

# 13	Which country has the lowest Diabetes prevalence?
diaLeast = df.select('Country').orderBy(col('Diabetes')).limit(1).show()

# 14	What is the average Birth Rate across all countries?
avgBirth = df.agg(round(avg('BirthRate'), 2).alias('average_birthrate')).show()

# 15	What is the average Death Rate across all countries?
deathRate = df.agg(round(avg('DeathRate'), 2).alias('avg_deathRate')).show()

# Question 1: First, let's identify the top 10 countries with the highest GDP per capita and their respective life expectancies. Then, we'll analyze the correlation between GDP per capita and life expectancy among these countries.

highGDP = df.select('Country', 'LifeExpectancy').orderBy(col('GDP').desc()).limit(10).show()

# Question 2: Next, we'll explore the relationship between GDP per capita and CO2 emissions per capita. We'll calculate the average CO2 emissions per capita for the top 5 countries with the highest GDP per capita and determine if there's any correlation between these two variables.

highGDP= df.select('Country', round(col('GDP')/col('Population'), 2).alias('gdp_per_capita')) \
    .orderBy(col('gdp_per_capita').desc()).limit(5)
highGDP.show()

highCO2 = df.select('Country', round(col('CO2')/col('Population'), 2).alias('co2_per_capita')) \
    .orderBy(col('co2_per_capita').desc()).limit(5).show()

corhigh = df.select('Country').orderBy(col('GDP').desc()).limit(5).show()

# What is the average Female Labor participation rate across all countries?
avgPop = df.agg(round(avg('FemaleLabor'), 2).alias('female_avg')).show()

# How many developed countries are there in the dataset
devCountry = df.where(col('Developed').isNotNull()).count()
print(devCountry)

nullCounty = df.where(col('Developed').isNull()).count()
print(nullCounty)

totalnull = df.count()
print(totalnull)

# 25	What is the average GDP per capita of developed countries?
avgallGDP = df.where(col('Developed').isNotNull()) \
        .agg(round(avg('GDP'), 2).alias('')).show()
