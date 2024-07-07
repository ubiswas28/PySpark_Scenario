# Databricks notebook source
# MAGIC %md
# MAGIC Day 1: PROBLEM STATEMENT: Calculate the distance travelled by the individuals who started at the zero spot.
# MAGIC
# MAGIC Define the schema for the journey table
# MAGIC schema = "daycount INT, distancecovered INT"
# MAGIC
# MAGIC Sample data
# MAGIC data = [(1, 20), (2, 40), (3, 90), (4, 30), (5, 0), (6, 90), (7, 40), (8, 10), (9, 0), (10, 40), (11, 10), (12, 0)]
# MAGIC
# MAGIC Expected Output:
# MAGIC TotalDistance 180 140 50

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from  pyspark.sql import functions as sf
from  pyspark.sql import *
from pyspark.sql.functions import sum, when
from pyspark.sql import Window

spark=SparkSession.builder.appName("Calculate distance travelled").getOrCreate()

# COMMAND ----------

data = [(1, 20), (2, 40), (3, 90), (4, 30), (5, 0), (6, 90), (7, 40), (8, 10), (9, 0), (10, 40), (11, 10), (12, 0)]
schema = "daycount INT, distancecovered INT"

# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()
windowSpec=Window.orderBy("daycount")
df=df.withColumn("group", sum(when(df.distancecovered==0,1).otherwise(0)).over(windowSpec.rowsBetween(Window.unboundedPreceding,0)))
df1=df.groupBy("group").agg(sum("distancecovered").alias("Totaldistance")).orderBy("group")
df1.filter(df1.Totaldistance>0).display()


# COMMAND ----------

# MAGIC %md 
# MAGIC # DAY 3 :PROBLEM STATEMENT: Look for only those companies whose revenue is rising annually. Let's say a company's revenue rises for three years, then drops the very following year. In that scenario, the revenue shouldn't be included in the output. 

# COMMAND ----------

import pyspark
from pyspark.sql import Window
from pyspark.sql.functions import lag
from pyspark.sql.window import Window
from pyspark.sql import SparkSession 
from pyspark.sql.types import *
from pyspark.sql import functions as F
spark=SparkSession.builder.appName("Rising_Revenue").getOrCreate()

schema=StructType([
    StructField("company",StringType(),True),
    StructField("year",IntegerType(),True),
    StructField("revenue",IntegerType(),True)])

#Data to insert into company revenue taable 

data =[   ('ABC1', 2000, 100), 
    ('ABC1', 2001, 110), 
    ('ABC1', 2002, 120), 
    ('ABC2', 2000, 100), 
    ('ABC2', 2001, 90), 
    ('ABC2', 2002, 120), 
    ('ABC3', 2000, 500), 
    ('ABC3', 2001, 400), 
    ('ABC3', 2002, 600), 
    ('ABC3', 2003, 800) ]


# COMMAND ----------

df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# Define a window partitioned by company and ordered by year
windowSpec=Window.partitionBy('company').orderBy('year')

# Calculate lagged revenue 
df=df.withColumn('prev_rev', lag('revenue').over(windowSpec))

# COMMAND ----------

df=df.withColumn("Rev_diff",df.revenue-df.prev_rev)
df.show()
result=df.groupBy('company').agg(F.min('Rev_diff').alias("Rev_Change"))
result1=result.filter(result.Rev_Change > 0 ).select('company').show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Hello, Connections ✨
# MAGIC
# MAGIC hashtag#DAY3
# MAGIC  
# MAGIC Here is the hashtag#Day3 of Our hashtag#Pyspark, hashtag#SparkSQL Learning Challenge.
# MAGIC  
# MAGIC Concepts for Data Engineer(PySpark, Spark SQL)
# MAGIC  
# MAGIC 𝐒𝐜𝐞𝐧𝐞𝐫𝐢𝐨 𝐰𝐢𝐭𝐡 𝐬𝐨𝐥𝐮𝐭𝐢𝐨𝐧
# MAGIC  
# MAGIC 𝐒𝐜𝐞𝐧𝐞𝐫𝐢𝐨:
# MAGIC The daily reported COVID cases for 2021 are shown in the table. Determine the monthly percentage rise in Covid cases compared to the total number of cases as of the previous month. Sort the result by month and return the month number along with the percentage increase rounded to one decimal place. 
# MAGIC
# MAGIC Try coming up with a new approach; I've included one in the post. Please double-check it.
# MAGIC  
# MAGIC Regards! 
# MAGIC Together, we can grow and learn. 
# MAGIC Please share this again with your network.
# MAGIC  
# MAGIC 𝐅𝐨𝐥𝐥𝐨𝐰: https://lnkd.in/ggBSP5RF for more…
# MAGIC  
# MAGIC Please check out our 𝕴𝖓𝖘𝖙𝖆𝖌𝖗𝖆𝖒 account for our latest Post: https://lnkd.in/gizfkVcy
# MAGIC
# MAGIC 𝐏𝐫𝐨𝐣𝐞𝐜𝐭 𝐥𝐢𝐧𝐤: @https://lnkd.in/gxjKWsXj

# COMMAND ----------

import pyspark
from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,DateType,StringType,StructField,StructType
from pyspark.sql import functions
from pyspark.sql.window import Window
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("Covid Percent Change").getOrCreate()

# COMMAND ----------

data =[('2021-01-01',66),('2021-01-02',41),('2021-01-03',54), ('2021-01-05', 16), ('2021-01-06', 90), ('2021-01-07', 34), 
('2021-01-08', 84),  ('2021-01-09', 71), ('2021-01-10', 14), ('2021-01-11', 48), ('2021-01-12', 72), ('2021-02-01', 38), ('2021-02-02', 57), ('2021-02-03', 42), ('2021-02-04', 61), ('2021-02-05', 25), ('2021-02-06', 78), ('2021-02-07', 33), ('2021-02-08', 93), ('2021-02-09', 62), ('2021-02-10', 15), ('2021-02-11', 52), ('2021-02-12', 76)]

schema=StructType([StructField("record_date",StringType(),True),StructField("cases_count",IntegerType(),True)])
df=spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

print(df.dtypes)

# COMMAND ----------

df=df.withColumn("date_column",to_date("record_date","yyyy-MM-dd"))
df.show()
print(df.dtypes)
cte_df=df.groupBy(month("date_column").alias("record_month")).agg(sum("cases_count").alias("total_cases"))
#cte_df.show()
cte_df2=cte_df.withColumn("prev_cases", lag("total_cases",1,0).over(Window.orderBy("record_month"))).withColumn("diff",col("total_cases")- col("prev_cases")).withColumn("percent_change",round(100 *(col("diff")/col("total_cases")),2)).select("record_month","total_cases","diff","percent_change")
cte_df2.show()
#.rowsBetween(Window.unboundedPreceding,Window.currentRow))
