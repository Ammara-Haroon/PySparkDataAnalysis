

from matplotlib import pyplot as plt
from pandas import Series
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Create SparkSession
spark = SparkSession.builder \
               .appName('SparkBillionaresAnalysis.com') \
               .getOrCreate()

csv_file = "World_Billionaire_2024.csv"
df = spark.read \
        .format("csv") \
        .option("header","true") \
        .load(csv_file)

df.createOrReplaceTempView("u")
df.describe().show()
df.printSchema()
df.show(5)

countries = df.select("Country")
countries.groupBy("Country").count().orderBy(desc("count")).limit(5).show()
df = df.dropna()
newdf=df.withColumn('Net Worth',regexp_replace("Net Worth","[$,B]",""))

newdf=newdf.withColumn('Net Worth',newdf['Net Worth'].cast("float").alias('Net Worth'))

newdf.filter(df.COUNTRY == "United States").orderBy(desc("Net Worth")).show(5)


hist_data = countries.groupBy("Country").count().orderBy(desc("count")).limit(7)
histogram = hist_data.toPandas().plot.bar(x="Country",y="count")

plt.show()
spark.stop()