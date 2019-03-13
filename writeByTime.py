'''
This script is an immediate successor of sortByTime.py and tokyo.ipynb.
This file serves to select data from sorted Docomo data and write them to txt files.
Two methods are included:
(1) Ordinary Python;
(2) Spark DataFrame;

Later I realized linear write is faster than Spark, so in tokyo.ipynb, linear write is used.

To use type in Terminal:
bin/spark-submit --master 'local[*]' /path-to-file/writeByTime.py

We have used the ordinary Python way, but it's kind of slow.
'''

import sys
import time
import os
import csv

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession

# Locate which csv file the designated searchTime is 
def locateTimeFile(searchTime, files, location):
    for index in range(location, len(files)):
        with open('/Volumes/My Passport for Mac/byTime/res/{}.csv'.format(files[index])) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            # line_count = 0
            for row in csv_reader:
                if row[0] == searchTime:
                    return index
                # line_count += 1
    return None

def sparkLocateTime(searchTime, files, location):

    for index in range(location, len(files)):
        spark = SparkSession.builder \
            .master("local[*]") \
            .config("spark.driver.memory", "8G") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        df = spark.read.format("csv") \
            .option("header","true") \
            .load('/Volumes/My Passport for Mac/byTime/res/{}.csv'.format(files[index]))
        df.createOrReplaceTempView("tmp")
        tmpDF = spark.sql("SELECT org_date FROM tmp WHERE org_date = '{}'".format(searchTime))
        spark.catalog.dropTempView("tmp")
        print(tmpDF.head(1))
        if len(tmpDF.head(1)) == 0:
            pass
        else:
            return index
    return None

# Ordinary Python iterate and write to txt file
def writeTimeFile(searchTime, files, location):
    f = open('/Volumes/My Passport for Mac/byTime/txt/{}.txt'.format(searchTime), 'a')
    with open('/Volumes/My Passport for Mac/byTime/res/{}.csv'.format(files[location])) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if row[0] == searchTime:
                f.write(row[1] + ',' + row[5] + ',' + row[8] + ',' + row[9] + '\n')
            line_count += 1
    f.close()
    if location != len(files)-1:
        with open('/Volumes/My Passport for Mac/byTime/res/{}.csv'.format(files[location+1])) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                line_count += 1
                if line_count > 2:
                    break
                else:
                    if row[0] == searchTime:
                        raise Exception('Time data not confined to one file')
    return 0

# Spark way of writing the data to txt file
def sparkWriteTime(searchTime, files, location):
    spark = SparkSession.builder \
        .master("local[*]") \
        .config("spark.driver.memory", "8G") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    df = spark.read.format("csv") \
        .option("header","true") \
        .load('/Volumes/My Passport for Mac/byTime/res/{}.csv'.format(files[location]))
    df.printSchema()
    df.createOrReplaceTempView("tmp")
    tmpDF = spark.sql("SELECT org_mesh, dest_mesh, trip_population, duration FROM tmp WHERE org_date = '{}'".format(searchTime))
    spark.catalog.dropTempView("tmp")
    # tmpDF.coalesce(1).write.format("text").option("header", "false").mode("append").save("/Volumes/My Passport for Mac/byTime/txt/{}.txt".format(searchTime))
    tmpDF.coalesce(1).write.option("header","true").option("inferSchema","true").csv("/Volumes/My Passport for Mac/byTime/txt/{}.csv".format(searchTime))
    return 0

# minTime = '2018-05-01 00:00:00'
minTime = '2018-05-02 22:20:00'
maxTime = '2018-05-02 22:20:00'
# maxTime = '2018-05-01 00:00:00'

directory = '/path-to-file/'
# the list of timestamps we use in the project
timeList = [line.rstrip('\n') for line in open(directory + 'timeList.txt', 'r')]

# os.walk considers sub-directory, but here this is enough
files = []
for file in os.listdir(directory + 'res/'):
    if file.endswith(".csv"):
        files.append(file[:-4])
    
location = 10
for searchTime in timeList:
    start = time.time()
    if searchTime < minTime or searchTime > maxTime:
        continue
    location = locateTimeFile(searchTime, files, location)
    # location = sparkLocateTime(searchTime, files, location)
    print('Finished locating {}'.format(searchTime))
    print("It's located at {}".format(location))
    stop = time.time()
    print('Searching time is {}'.format(stop - start))
    start = time.time()
    if location is not None:
        # _ = writeTimeFile(searchTime, files, location)
        _ = sparkWriteTime(searchTime, files, location)
        stop = time.time()
        print('It took {} seconds in Spark to write'.format(stop - start))