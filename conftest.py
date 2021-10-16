from __future__ import absolute_import
import pytest
from tests import setup
import os
import sys
from pyspark.sql import SparkSession
    

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("nasa_log_analysis") \
        .getOrCreate()
    
    return spark