#!/usr/bin/env python3.7
# ---------------------------------------------------------------------------------------------------#
# /Users/klluzbet/apache-spark/21-generate-data-ddl.py
# https://faker.readthedocs.io/en/master/
# ---------------------------------------------------------------------------------------------------#
from __future__ import print_function

from faker import Faker
import pprint
import random, os, sys, pyspark
from datetime import timedelta, timezone, datetime

import hashlib
import re
import yaml
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import SQLContext
import pandas as pd
import numpy as np
from pyspark.sql.types import *

pp = pprint.PrettyPrinter(width=41, compact=True)
# ---------------------------------------------------------------------------------------------------#
filename = "s3a://luzbetak/parquet-14"

# ---------------------------------------------------------------------------------------------------#
access_key = os.environ.get('ACCESS_KEY')
secret_key = os.environ.get('SECRET_KEY')

# ---------------------------------------------------------------------------------------------------#
# os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.7"
os.environ["SPARK_HOME"] = "/usr/local/spark-2.4.4-bin-hadoop2.7/"
# os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.7 pyspark-shell"

# --- Create Spark Session, Context and SQL ---#
spark = SparkSession.builder.appName("DataGenerator").config("spark.sql.crossJoin.enabled", "true").getOrCreate()

sc = spark.sparkContext
sc.setSystemProperty("spark.python.worker.memory", "4g")
sc.setSystemProperty("spark.driver.cores", "4")
sc.setSystemProperty("spark.driver.memory", "4g")
sc.setSystemProperty("spark.executor.memory", "1g")
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
sc.setSystemProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# sqlContext = SQLContext(sc)

# --- Amazon S3 Configuration ---#
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)
hadoop_conf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")

# ---------------------------------------------------------------------------------------------------#
def random_date():
    step = timedelta(days=1)
    start = datetime(2013, 1, 1, tzinfo=timezone.utc)
    end = datetime.now(timezone.utc)
    random_date = start + random.randrange((end - start) // step + 1) * step

    return random_date


# ---------------------------------------------------------------------------------------------------#
def random_sentence(words=10):
    sentence = ''
    lines = open('/usr/share/dict/words').read().splitlines()

    for a in range(words):
        line = random.choice(lines)
        sentence = sentence + " " + line

    return sentence


# ---------------------------------------------------------------------------------------------------#
def load_yaml_ddl(yaml_ddl):
    with open(yaml_ddl) as file:
        documents = yaml.full_load(file)

    # pp.pprint(documents)

    # v = documents.items()
    # pp.pprint(v)

    # -------------------------------------- #
    #        Display Field List
    # -------------------------------------- #
    # field_list = documents.get("data")
    # for i in field_list:
    #     print(i.get("name"))

    # for item, doc in documents.items():
    # print(item, ":", doc)
    #    pp.pprint(doc)

    field_array = []
    for i in documents.get("data"):
        field_array.append(i.get("name") + "|" + i.get("data_type") + "|" + str(i.get("length")))

    return field_array


# -------------------------------------------------------------------------------------------------- #
def get_fake_id(keyword):
    simple256 = hashlib.sha256(keyword.encode('utf-8')).hexdigest()
    return simple256[50:]


# ---------------------------------------------------------------------------------------------------#
# Prototype to generate sample data: 
#    1. Generate client synthetic data into a list by appending the list
#    2. Parallelize list into Resilient Distributed Datasets (RDD)
#    3. Create DataFrame from the RDD
#    4. Save the DataFrame incrementally into S3 parquet format
#    5. Repeat 100 million times from 100+ servers running concurrently
#    6. Copy parquet format into Redshift Cluster
# ---------------------------------------------------------------------------------------------------#
# https://spark.apache.org/docs/latest/configuration.html
# ---------------------------------------------------------------------------------------------------#
def generate_sample_data(records):

    # --- Dynamically build set of records --- #
    faker = Faker()
    field_name = []
    list1 = []  # list1 = [('0', '0', 0, 0), ('0', '0', 0, 0)]

    list1 = [('0', '0', '0', '0'), ('0', '0', '0', '0')]
    mySchema = StructType([  StructField("Col1", StringType(), True)\
                           , StructField("Col2", StringType(), True)\
                           , StructField("Col3", StringType(), True)\
                           , StructField("Col4", StringType(), True)])

    df = spark.createDataFrame(list1,schema=mySchema)
    df.printSchema()

    # for y in range(0, 10):
    #    # df.printSchema()
    #    print(type(df))

    df.write.parquet(filename, mode="append") 

    # --- Read Parquet from S3 ---#
    # data = spark.read.parquet(filename)
    # data.show(25, False)
    # ------------ End of the Chunk Generation ---------------------------------#

    # print(sc._conf.getAll())
    # SparkContext.stop(sc)


# ---------------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    # generate_sample_data()
    fields = load_yaml_ddl("90-ddl/02-test-ddl.yaml")
    print(fields)
    print(len(fields))
    generate_sample_data(fields)
    
    spark.stop()
# ---------------------------------------------------------------------------------------------------#
