# -*- coding: utf-8 -*-
"""
Spark_Submit:
export SPARK_MAJOR_VERSION=3
spark-submit --master local[*] --num-executors 1 --executor-cores 2 --executor-memory 1G --driver-memory 1G
--properties-file /nasa_log_conf.properties /nasa_log_analysis.py
@author: nyamani
"""
from os import truncate
try:
    import logging
    import os
    import sys
    import datetime
    from pyspark import SparkContext
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.sql.functions import coalesce, col, to_date, regexp_extract, udf, dense_rank
    from pyspark.sql.window import Window
    import pyspark.sql.functions as F
    import requests
    from typing import Iterable
    print("Spark Libraries Imported Successfully")
except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)

def getSparkSessionInstance(appname):
    """ This function Creates SparkSession Instance if it does not exists"""
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .appName(appname).enableHiveSupport() \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def fileExists(filePath):
    """This func takes file path as input and Checks if the file exist and returns boolean"""
    return os.path.exists(filePath)

def readInputFile(inputFilePath, Fformat):
    """ This function takes two args: inputFilePath, FileFormat and returns a Spark DF"""
    if fileExists(inputFilePath):
        try:
            df = spark.read.format(Fformat).load(inputFilePath)
        except IOError as e:
            print("Exception while reading the file: ", e)
    return df

def columnExtract(df):
    """ This function takes a DF as input and returns a log parsed DF using regex """
    host_regex = r'(^\S+\.[\S+\.]+\S+)\s'
    timestamp_regex = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
    method_uri_protocol_regex = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    status_regex = r'\s(\d{3})\s'
    content_size_regex = r'\s(\d+)$'
    df = df.select(regexp_extract('value', host_regex, 1).alias('host'),
                        regexp_extract('value', timestamp_regex, 1).alias('timestamp'),
                        regexp_extract('value', method_uri_protocol_regex, 1).alias('method'),
                        regexp_extract('value', method_uri_protocol_regex, 2).alias('url'),
                        regexp_extract('value', method_uri_protocol_regex, 3).alias('protocol'),
                        regexp_extract('value', status_regex, 1).cast('integer').alias('status'),
                        regexp_extract('value', content_size_regex, 1).cast('integer').alias('contentSize'))
    return df

month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10,
             'Nov': 11, 'Dec': 12}

def parse_log_time(s):
    """ Convert Apache time format into a Python timestamp string format
    Args:
        s (str): date and time in Apache time format
    Returns:
        timestamp: timestamp in string format.
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20])).strftime('%Y-%m-%d %H:%M:%S')

def route_parsed_dataframes(df):
    """ This function do a null check and returns two dataframes
    Args:
        df: spark dataframe
    Returns:
        df,df: A good data Frame with out nulls in Status field and an error or bad data Frame with nulls in any of the fields"""
    bad_rows_df = df.filter(' OR '.join([c + " is null" for c in df.columns]))
    good_rows_df = df.filter(df['status'].isNotNull())
    return good_rows_df, bad_rows_df

def top_n_visitors_day(df, topnumber):
    """ This function aggregates host or visitor data and returns the top N frequent visitors for each day
    Args:
        df : spark Data Frame
        topnumber: top-N
    Returns:
        df : Aggregated, ranked df with top-N visitors each day"""
    agg_visitor_df = df.groupBy('host', 'date').count()
    w = Window.partitionBy('date').orderBy(desc('count'))
    frequent_visitors = agg_visitor_df.select('host', 'date', 'count', dense_rank().over(w).alias('DR')).where(
        'DR <= {}'.format(topnumber))
    return frequent_visitors

def top_n_urls_day(df, topNumber):
    """  This function aggregates url data and returns the top N frequent urls for each day
    Args:
        df : spark Data Frame
        topnumber: top-N
    Returns:
        df : Aggregated, ranked df with top-N urls each day"""
    agg_url_df = df.groupBy('url', 'date').count()
    w = Window.partitionBy('date').orderBy(desc('count'))
    frequent_urls = agg_url_df.select('url', 'date', 'count', dense_rank().over(w).alias('DR')).where(
        'DR <= {}'.format(topNumber))
    return frequent_urls

def top_n_visitors_urls_day(df, topNumber):
    """  This function aggregates data and returns the top N frequent visitors and urls for each day
    Args:
        df : spark Data Frame
        topnumber: top-N
    Returns:
        df : Aggregated, ranked df with top-N visitors and urls each day"""
    agg_host_url_df = df.groupBy('host', 'url', 'date').count()
    w = Window.partitionBy('date').orderBy(desc('count'))
    frequent_visitor_urls = agg_host_url_df.select('host', 'url', 'date', 'count',
                                                   dense_rank().over(w).alias('DR')).where('DR <= {}'.format(topNumber))
    return frequent_visitor_urls

def save_df(df, path, fileformat, partitionnbr):
    """This function writes data into file system in desired format with desired number of files
    Args:
        df: Data Frame to be written to FS
        path: output Directory
        fileformat: Output file format like csv,json,parquet,avro and delta
        partitionnbr: Number of files to be created
    """
    try:
        df.repartition(int(partitionnbr)).write.option('header', True).mode('overwrite').format(fileformat).save(path)
    except IOError as e:
        print("Exception while writing the file: ", e)

if __name__ == "__main__":
    conf = SparkConf()
    #sc = SparkSession.sparkContext
    sc = SparkContext(conf=conf)
    appname = "NasaLogAnalysis"

    # Create Spark instance to interact with Spark engine.
    spark = getSparkSessionInstance(appname)
    print("Spark Driver created for this session: ", spark)

    # Reading configurations from a Properties file.
    inputFilePath = sc.getConf().get("spark.infilePath")
    inputFileFormat = sc.getConf().get("spark.inputFileFormat")
    topNumber = int(sc.getConf().get("spark.topNumber"))
    visitorOutPutFile = sc.getConf().get("spark.visitorOutPutFile")
    urlOutPutFile = sc.getConf().get("spark.urlOutPutFile")
    visitorUrlOutPutFile = sc.getConf().get("spark.visitorUrlOutPutFile")
    badRecordsOutPutFile = sc.getConf().get("spark.badRecordsOutPutFile")
    outPutFileFormat = sc.getConf().get("spark.outPutFileFormat")
    partitionnbr = sc.getConf().get("spark.partitionNbr")
    print("Here is the input file path to read from : ", inputFilePath)

    # Reading .zip/text formatted log file into a Dataframe using Spark Read API.
    nasa_logDF = readInputFile(inputFilePath, inputFileFormat)

    # Extracting columns from each record/row in the DF.
    extractDF1 = columnExtract(nasa_logDF)

    # Optimization: Caching Data Frame to avoid reading source file multiple times when an action is called.
    extractDF, badDF = route_parsed_dataframes(extractDF1)
    extractDF.cache()

    # Formatting date into desired datetime format
    udf_parse_time = udf(parse_log_time)
    formattedDF = extractDF.withColumn("timestamp", udf_parse_time(col("timestamp")).cast('timestamp')).withColumn(
        "date", to_date(col("timestamp")))

    # top-N most frequent visitors for each day.
    frequent_visitors_day = top_n_visitors_day(formattedDF, topNumber)

    # top-N most frequent urls for each day.
    frequent_urls_day = top_n_urls_day(formattedDF, topNumber)

    # Assumption: Not sure if the question is for a combined analytic so creating a new DF with frequent visitors and urls for each day.
    frequent_visitors_urls = top_n_visitors_urls_day(formattedDF, topNumber)

    # Saving the DataFrames into File System.
    save_df(frequent_visitors_day, visitorOutPutFile, outPutFileFormat, partitionnbr)
    save_df(frequent_urls_day, urlOutPutFile, outPutFileFormat, partitionnbr)
    save_df(frequent_visitors_urls, visitorUrlOutPutFile, outPutFileFormat, partitionnbr)
    save_df(badDF, badRecordsOutPutFile, outPutFileFormat, partitionnbr)

    # Stopping the Driver hook.
    spark.stop()