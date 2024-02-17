from pyspark.sql import *

if __name__=="__main__":

    spark = SparkSession.builder.appName("spark").master("local[2]").getOrCreate()

    df = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("C:\projects\pycharm\data\Fire_Department_Calls_for_Service.csv")

    # print schema for DDL
    df.printSchema()

    # Creating a view on top of DataFrame df
    df.createOrReplaceTempView("fire_department")

    # Print a few lines of data for an overview
    df.show(20, truncate=False)

    # Solving problems using Spark SQL
    # Question 1: Distinct Call types
    distinct_call = spark.sql("SELECT DISTINCT `Call Type` FROM fire_department")
    distinct_call.show(10, truncate=False)

    # Question 2: Delay type more than 5 sec
    delay_time = spark.sql("""
    SELECT 
        `Call Number`,
        `Received DtTm`,
        `Response DtTm`,
        CAST(UNIX_TIMESTAMP(`Response DtTm`, 'MM/dd/yyyy HH:mm:ss') - UNIX_TIMESTAMP(`Received DtTm`, 'MM/dd/yyyy HH:mm:ss') AS INT) AS delay
    FROM fire_department
    """)

    delay_time.show(5, truncate=False)
    delay_time.createOrReplaceTempView("temp_delay")

    final_delay = spark.sql("""SELECT `Call Number`, delay FROM temp_delay
                            WHERE delay/60 > 5""")
    final_delay.show(10, truncate=False)
    print(final_delay.count())

    # Question 3: Most Common Call Types

    most_common_call = spark.sql("""SELECT `Call type`, COUNT(*) as total_call 
                                 FROM fire_department GROUP BY 1 ORDER BY 2 DESC LIMIT 1""")

    most_common_call.show()






