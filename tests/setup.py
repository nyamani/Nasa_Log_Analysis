import os
import sys

# Spark Setup for local:
# os.environ["PYSPARK_PYTHON"] = "C:\\Users\\nyamani\\Anaconda3\\python.exe"
# os.environ["SPARK_HOME"] = "C:\\Users\\nyamani\\Documents\\Nyamani2019\\TMO_BKP\\Spark_Home\\spark-2.3.0-bin-hadoop2.7"
# os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
# sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
# sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

# Spark setup for Docker
os.environ["PYSPARK_PYTHON"] = 'python3'
os.environ["SPARK_HOME"] = "/opt/spark"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.9-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

# from pyspark import SparkConf
from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()
# print(spark)