"""
test_nasa_log.py
~~~~~~~~~~~~~~~
This module contains pytest unit tests for the parsing, transformation and aggregation steps of the Nasa
log analysis job defined in nasa_log_analysis.py. It makes use of a local version of PySpark
that is built ."""

from __future__ import absolute_import
import pytest
#from tests import setup
import os
import sys
#sys.path.insert(1, 'C:\\Users\\nyamani\\Pyspark_Docker\\Nasa_Log_Analysis\\src')
from src.nasa_log_analysis import columnExtract, readInputFile, fileExists, parse_log_time, top_n_visitors_day, top_n_urls_day, route_parsed_dataframes
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import StructType, StringType, IntegerType, DateType,StructField,LongType
#import pandas as pd
import datetime
from chispa.dataframe_comparer import *

# spark = SparkSession.builder.getOrCreate()
# # print(spark)
# print("SparkSession successfully created....!")

print("Actual test cases starting here ..........!")

# Spark fistures are created in the conftest and refrenced here as args to test functions.
@pytest.mark.usefixtures("spark")
def test_fileExists(spark):
    """ test fileExists - Passing a test file or some random file path to the file exists function
    we test what fileExists.py returns with expected output.
    Args:
       Spark: test fixture SparkSession
    """
    #test_fileinputPath = "C:\\Users\\nyamani\\Pyspark_Docker\\Nasa_Log_Analysis\\sparkJob_submit.sh"
    test_fileinputPath = "/app/sparkJob_submit.sh"
    real_result = fileExists(test_fileinputPath)
    #expected_results = os.path.exists(test_fileinputPath)

    assert real_result==True
@pytest.mark.usefixtures("spark")
def test_columnExtract(spark):
    """ test test_columnExtract
    Using few sample input records of log data and expected output data, we test the columnExtract(regexp extract was used to extract column from each log record) transformation step to make sure it's working as expected.
    Args:
        Spark: test fixture SparkSession
    """
    # Creating a DF with some sample test data.
    src_df=spark.createDataFrame( [Row(value='199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245'), 
    Row(value='unicomp6.unicomp.net - - [01/Jul/1995:00:00:06 -0400] "GET /shuttle/countdown/ HTTP/1.0" 200 3985'), 
    Row(value='199.120.110.21 - - [01/Jul/1995:00:00:09 -0400] "GET /shuttle/missions/sts-73/mission-sts-73.html HTTP/1.0" 200 4085'), 
    Row(value='burger.letters.com - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/countdown/liftoff.html HTTP/1.0" 304 0'), 
    Row(value='199.120.110.21 - - [01/Jul/1995:00:00:11 -0400] "GET /shuttle/missions/sts-73/sts-73-patch-small.gif HTTP/1.0" 200 4179'), 
    Row(value='burger.letters.com - - [01/Jul/1995:00:00:12 -0400] "GET /images/NASA-logosmall.gif HTTP/1.0" 304 0') ], ['value'])
    
    # Creating/Mocking up expected data/result
    expected_df = spark.createDataFrame(
           [('199.72.81.55', '01/Jul/1995:00:00:01 -0400', 'GET', '/history/apollo/', 'HTTP/1.0', 200,6245),
            ('unicomp6.unicomp.net', '01/Jul/1995:00:00:06 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 3985),
            ('199.120.110.21', '01/Jul/1995:00:00:09 -0400', 'GET', '/shuttle/missions/sts-73/mission-sts-73.html', 'HTTP/1.0', 200, 4085),
            ('burger.letters.com', '01/Jul/1995:00:00:11 -0400', 'GET', '/shuttle/countdown/liftoff.html', 'HTTP/1.0', 304, 0),
            ('199.120.110.21', '01/Jul/1995:00:00:11 -0400', 'GET', '/shuttle/missions/sts-73/sts-73-patch-small.gif', 'HTTP/1.0', 200, 4179),
            ('burger.letters.com', '01/Jul/1995:00:00:12 -0400', 'GET', '/images/NASA-logosmall.gif', 'HTTP/1.0', 304, 0)],
            ['host','timestamp','method','url','protocol','status','contentSize']
     ).selectExpr('host','timestamp','method','url','protocol','cast(status as int)','cast(contentSize as int)')

     # Test transformation - Let's pass the test data to columnExtract function and get a new DF
    actual_df = columnExtract(src_df)

    assert_df_equality(actual_df, expected_df) 

@pytest.mark.usefixtures("spark")
def test_route_parsed_dataframes(spark):
    """ test route_parsed_dataframes
    Using few sample input records of log data with extracted columns(regex output) and expected output data, we test the route_parsed_dataframes(checks each columns if there are any nulls due to regex extract and filters them into another dataframe for further action and make sure job is not failing due to any emty or null values.) transformation step to make sure it's working as expected.
    Args:
        Spark: test fixture SparkSession
    """
    # Creating a DF with some sample test data. 

    src_df = spark.createDataFrame(
           [('199.72.81.55', '01/Jul/1995:00:00:01 -0400', 'GET', '/history/apollo/', 'HTTP/1.0', '200','6245'),
            ('unicomp6.unicomp.net', '01/Jul/1995:00:00:06 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', '200', '985'),
            ('199.120.110.21', '01/Jul/1995:00:00:09 -0400', 'GET', '/shuttle/missions/sts-73/mission-sts-73.html', 'HTTP/1.0', None, '4085'),
            ('', '', '', '', '', None, '')],
            ['host','timestamp','method','url','protocol','status','contentSize']
     )

    # Creating/Mocking up expected data/result
    schema_e = StructType([StructField('host',StringType(),True),
    StructField('timestamp',StringType(),True),
    StructField('method',StringType(),True),
    StructField('url',StringType(),True),
    StructField('protocol',StringType(),True),
    StructField('status',StringType(),True),
    StructField('contentSize',StringType(),True)
    ]) #['host','timestamp','method','url','protocol','status','contentSize']
    expected_df = spark.createDataFrame(
        [('199.120.110.21', '01/Jul/1995:00:00:09 -0400', 'GET', '/shuttle/missions/sts-73/mission-sts-73.html', 'HTTP/1.0', None, '4085'),
        ('', '', '', '', '', None, '')],schema_e
    )
   

    # Test transformation - Let's pass the test data to route-parsed_dataframes function and see if it retunrs two bad records into bad df
    actual_good_df,actual_bad_df = route_parsed_dataframes(src_df)

    assert_df_equality(actual_bad_df, expected_df) 

@pytest.mark.usefixtures("spark")
def test_parse_log_time(spark):
    """ test - parse_log_time
    Using a sample of records with date string in apache log format and expected output date format ('%Y-%m-%d %H:%M:%S'), to test parse log time function.
    Args:
        Spark: test fixture SparkSession
    """
    # Creating Sample input to test trasformation.
    src_df = spark.createDataFrame(
           [('199.72.81.55', '01/Jul/1995:00:00:01 -0400', 'GET', '/history/apollo/', 'HTTP/1.0', 200,6245),
            ('unicomp6.unicomp.net', '01/Jul/1995:00:00:06 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 3985),
            ('199.120.110.21', '01/Jul/1995:00:00:09 -0400', 'GET', '/shuttle/missions/sts-73/mission-sts-73.html', 'HTTP/1.0', 200, 4085),
            ('burger.letters.com', '01/Jul/1995:00:00:11 -0400', 'GET', '/shuttle/countdown/liftoff.html', 'HTTP/1.0', 304, 0),
            ('199.120.110.21', '01/Jul/1995:00:00:11 -0400', 'GET', '/shuttle/missions/sts-73/sts-73-patch-small.gif', 'HTTP/1.0', 200, 4179)],
            ['host','timestamp','method','url','protocol','status','contentSize']
     ).select('timestamp')

    udf_parse_time = udf(parse_log_time)
    
    # Transformation step accepting/taking input data in desired format.
    actual_df = src_df.withColumn('timestamp', udf_parse_time(col('timestamp'))) # UDF has been created out of log_pase_time to make use of it as a spark function.
    
    # Creating/Mocking up expected data/result
    expected_df = spark.createDataFrame([Row(value='1995-07-01 00:00:01'),Row(value='1995-07-01 00:00:06'),Row(value='1995-07-01 00:00:09'),Row(value='1995-07-01 00:00:11'),Row(value='1995-07-01 00:00:11')],
     ['timestamp'])
    assert_df_equality(actual_df, expected_df)

@pytest.mark.usefixtures("spark")
def test_top_n_visitors_day(spark):
    """ test top_n_visitors_day
    Using few sample input records of log data and expected output data, we test the top_n_visitors_day aggregation and ranking transformation (Determines top n frequent host/visiotrs for each date/day) step to make sure it's working as expected.
    Args:
        Spark: test fixture SparkSession
    """
    # Creating Sample input to test trasformation.
    sample_df = spark.createDataFrame(
           [('burger.letters.com', '01/Jul/1995:00:00:01 -0400', 'GET', '/history/apollo/', 'HTTP/1.0', 200,6245),
            ('unicomp6.unicomp.net', '01/Jul/1995:00:00:06 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 3985),
            ('199.120.110.21', '01/Jul/1995:00:00:09 -0400', 'GET', '/shuttle/missions/sts-73/mission-sts-73.html', 'HTTP/1.0', 200, 4085),
            ('burger.letters.com', '01/Jul/1995:00:00:11 -0400', 'GET', '/shuttle/countdown/liftoff.html', 'HTTP/1.0', 304, 0),
            ('199.120.110.21', '01/Jul/1995:00:00:11 -0400', 'GET', '/shuttle/missions/sts-73/sts-73-patch-small.gif', 'HTTP/1.0', 200, 4179)],
            ['host','timestamp','method','url','protocol','status','contentSize']
     )
    udf_parse_time = udf(parse_log_time)
    src_df = sample_df.withColumn('timestamp', udf_parse_time(col('timestamp')).cast('timestamp')).withColumn("date", to_date(col("timestamp")))

    schema1 = StructType([StructField('host',StringType(),True),StructField('date',StringType(),True),StructField('count',LongType(),False),StructField('DR',IntegerType(),True)])

    # Creating/Mocking up expected data/result
    expected_df = spark.createDataFrame(
        [('199.120.110.21', '1995-07-01', 2, 1),
        ('burger.letters.com', '1995-07-01', 2, 1),
        ('unicomp6.unicomp.net', '1995-07-01', 1, 2)],
        schema1
    ).selectExpr('host','cast(date as date)','count','cast(DR as int)').orderBy('count','host', ascending=False)
    
    # Transformation step accepting/taking input data in desired format.
    actual_df = top_n_visitors_day(src_df, 3)
    
    # Asserting actual result of input data vs expected reult
    assert_df_equality(actual_df, expected_df)

@pytest.mark.usefixtures("spark")
def test_top_n_urls_day(spark):
    """ test top_n_urls_day
    Using few sample input records of log data and expected output data, we test the top_n_urls_day aggregation and ranking transformation (groups records on date and urls and then rank them based on frequency/count. finally results in the no of records user wants to see for each day) step to make sure it's working as expected.
    Args:
        Spark: test fixture SparkSession
    """
    #Sample input creation to pass as input to tranformation step.
    sample_df = spark.createDataFrame(
           [('burger.letters.com', '01/Jul/1995:00:00:01 -0400', 'GET', '/history/apollo/', 'HTTP/1.0', 200,6245),
            ('unicomp6.unicomp.net', '01/Jul/1995:00:00:06 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 200, 3985),
            ('199.120.110.21', '01/Jul/1995:00:00:09 -0400', 'GET', '/shuttle/missions/sts-73/mission-sts-73.html', 'HTTP/1.0', 200, 4085),
            ('burger.letters.com', '01/Jul/1995:00:00:11 -0400', 'GET', '/shuttle/countdown/', 'HTTP/1.0', 304, 0)],
            ['host','timestamp','method','url','protocol','status','contentSize']
     )
    udf_parse_time = udf(parse_log_time)
    src_df = sample_df.withColumn('timestamp', udf_parse_time(col('timestamp')).cast('timestamp')).withColumn("date", to_date(col("timestamp")))
    schema1 = StructType([StructField('url',StringType(),True),StructField('date',StringType(),True),StructField('count',LongType(),False),StructField('DR',IntegerType(),True)])
    
    # Creating/Mocking up expected data/result
    expected_df = spark.createDataFrame(
        [('/shuttle/countdown/', '1995-07-01', 2, 1),
        ('/history/apollo/', '1995-07-01', 1, 2),
        ('/shuttle/missions/sts-73/mission-sts-73.html', '1995-07-01', 1, 2)],
        schema1
    ).selectExpr('url','cast(date as date)','count','cast(DR as int)').sort(col('count').desc(),col('url').asc())

    # Transformation step accepting/taking input data in desired format.
    actual_df = top_n_urls_day(src_df, 3)
    assert_df_equality(actual_df, expected_df.sort(col('count').desc(),col('url').asc()))