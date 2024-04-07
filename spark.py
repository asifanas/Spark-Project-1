from pyspark.sql import *
import pyspark
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("spark").config("spark.sql.legacy.timeParserPolicy", "LEGACY").master("local[3]").getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("C:\projects\pycharm\data\hollywoodmovies.csv")

    # print schema for DDL
df.printSchema()

    # Creating a view on top of DataFrame df
df.createOrReplaceTempView("hollywoodmovies")

    # Print a few lines of data for an overview
df.show(20, truncate=False)
    # Solving problems using Spark SQL
    # Question 1: Distinct Call types
    # distinct_call = spark.sql("SELECT DISTINCT `Call Type` FROM fire_department")
    # distinct_call.show(10, truncate=False)
    #
    # # Question 2: Delay type more than 5 sec
    # delay_time = spark.sql("""
    # SELECT
    #     `Call Number`,
    #     `Received DtTm`,
    #     `Response DtTm`,
    #     CAST(UNIX_TIMESTAMP(`Response DtTm`, 'MM/dd/yyyy HH:mm:ss') - UNIX_TIMESTAMP(`Received DtTm`, 'MM/dd/yyyy HH:mm:ss') AS INT) AS delay
    # FROM fire_department
    # """)
    #
    # delay_time.show(5, truncate=False)
    # delay_time.createOrReplaceTempView("temp_delay")
    #
    # final_delay = spark.sql("""SELECT `Call Number`, delay FROM temp_delay
    #                         WHERE delay/60 > 5""")
    # final_delay.show(10, truncate=False)
    # print(final_delay.count())
    #
    # # Question 3: Most Common Call Types
    #
    # most_common_call = spark.sql("""SELECT `Call type`, COUNT(*) as total_call
    #                              FROM fire_department GROUP BY 1 ORDER BY 2 DESC LIMIT 1""")
    #
    # most_common_call.show()
    #
    # # # Question 4: Zip Codes for Most Common Calls
    # zip_code = spark.sql("""SELECT `Zipcode of Incident` as zip_code FROM
    #                     fire_department WHERE `Call type` = (SELECT `Call type`
    #                     FROM (SELECT `Call type`, COUNT(*) as total_call
    #                     FROM fire_department GROUP BY 1 ORDER BY 2 DESC LIMIT 1) cust) LIMIT 1""")
    #
    # zip_code.show()
    #
    # #Question 5: San Francisco Neighborhoods in Zip Codes 94102 and 94103 trying to solve using pyspark sql
    #
    # #Question 8: Distinct Years of Data
    #
    # Distinct_year = spark.sql("""SELECT YEAR(data_loaded_at) AS year, COUNT(*) AS count
    # FROM fire_department
    # GROUP BY YEAR(data_loaded_at)
    # ORDER BY year""")
    #
    # Distinct_year.show(20, truncate=False)
    #
    # #Question 9: Week of the Year with Most Fire Calls in 2018
    #
    # week_year = spark.sql("""
    # SELECT WEEKOFYEAR(`call date`) AS week_of_year, COUNT(*) AS call_count
    # FROM fire_department
    # WHERE to_date(`call date`, 'MM/dd/yyyy') BETWEEN '2018-01-01' AND '2018-12-31'
    # GROUP BY WEEKOFYEAR(`call date`)
    # ORDER BY call_count DESC
    # """)
    #
    # week_year.show(10, truncate=False)

#try to find out those movies whose audience score more than 80
audience = df.where(col('AudienceScore') > 80) \
             .select('Movie', 'AudienceScore') \
             .orderBy(col('Movie'))
# audience.show()

lead = df.where(col('LeadStudio') == 'Sony') \
        .select('Movie', 'LeadStudio')

#lead.show(10)

# What are the top 5 movies with the highest Rotten Tomatoes scores?
rotten = df.orderBy(col('RottenTomatoes').desc()) \
        .select('Movie') \
        .limit(5)
rotten.show()

# Which movie had the highest Opening Weekend gross?

opening = df.orderBy(col('OpeningWeekend').desc()) \
        .select('Movie') \
        .limit(1)
opening.show()

# How many movies were released in each year?
eachYear = df.groupby('Year').count()
eachYear = eachYear.withColumnRenamed("count", "number of movies")
eachYear.show()

# What is the average Audience Score for movies released by each Lead Studio?
audscore = df.groupby('LeadStudio').agg(avg('AudienceScore').alias("average_audience_score"))
audscore.show()

# What is the average Domestic Gross for movies in each Genre?
avgGenre = df.groupby('Genre').agg(round(avg('DomesticGross'), 2).alias("domestic gross for each genre"))
avgGenre.show()

# How many movies fall into each Genre category?
catGenre = df.groupby('Genre').agg(count('Movie').alias('number of movies'))
catGenre.show()

# Which movie had the highest Profitability?
mostProfit = df.select('Movie').orderBy(col('Profitability').desc()).limit(1)
mostProfit.show()