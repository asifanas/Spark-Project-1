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
mostnumber = mostStadium.where(col('Stadium').isNotNull()) \
        .select('Stadium', 'total_number').limit(1).show()

# 5	How many goals were scored in total across all matches?
totalGoals = df.agg(sum(col('Home Team Goals') + col('Away Team Goals')).alias('total'))
totalGoals.show()

# 6	What is the average number of goals per match?
avgGoal = totalGoals.select(col('total')).collect()[0][0]/df.count()
print(avgGoal)

# 7	Which referee officiated the most matches?
refMost = df.groupby('Referee').agg(count('*').alias('total')).orderBy(col('total').desc()).limit(5)
refMost1 = refMost.where(col('Referee').isNotNull()).select('Referee').limit(1).show()

# 8	Calculate the total number of goals scored by each team.
t_goal = df.groupby('Home Team Name').agg(sum(col('Home Team Goals').alias('total1')))
t_goal1 = df.groupby('Away Team Name').agg(sum(col('Away Team Goals').alias('total2')))

joined_df = t_goal.join(t_goal1, t_goal['Home Team Name'] == t_goal1['Away Team Name'], 'inner').groupby('Home Team Name').agg(sum(col('sum(Home Team Goals AS total1)') + col('sum(Away Team Goals AS total2)')).alias('total_goals_by_team')).show()

# 9	Determine the match with the highest attendance.









