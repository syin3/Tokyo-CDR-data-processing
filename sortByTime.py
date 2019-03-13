'''
This file sort the original csv dataset in time, using Spark SQL.
The commands are written for single machine. In clusters, please make appropriate changes.

To use type in Terminal:
bin/spark-submit --master 'local[*]' /path-to-file/sortByTime.py

Configurations available:
https://spark.apache.org/docs/latest/configuration.html#available-properties

Java version (in terminal):
export JAVA_HOME=`/usr/libexec/java_home -v "1.8"`
'''

import sys
import time

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
# from pyspark.sql.functions import to_timestamp

def process(df, castDateType = True):
    '''
    Originally thought sorting by string does not give the right order (0:0:0 does not appear at head).
    Actually there is no record earlier than that.
    '''
    if castDateType:
        typeChangedDF = df.withColumn("org_date", df["org_date"].cast("timestamp")) # yyyy-MM-dd HH:mm:ss
        sortedDF = typeChangedDF.sort("org_date", ascending=True)
    else:
        sortedDF = df.sort("org_date", ascending=True)
    # print("The earliest record is: {}.\n".format(sortedDF.agg({"org_date": "min"}).collect()[:3]))
    print("The latest record is: {}.\n".format(sortedDF.agg({"org_date": "max"}).collect()[:3]))
    if castDateType:
        finalDF = sortedDF.withColumn("org_date", sortedDF["org_date"].cast("string"))
        return finalDF
    else:
        return sortedDF

# Can definitely try:
# "spark.driver.memory", "8G"
spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.memory", "8G") \
        .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# .config("spark.some.config.option", "some-value")

df = spark.read.format("csv") \
    .option("header","true") \
    .load('/Volumes/My Passport for Mac/trip_od_v3_201805/trip_od_v3_201805.csv')

df.printSchema()

outputDF = process(df, False)
outputDF.write.option("header","true").option("inferSchema","true").format("csv").save('./sort/')



