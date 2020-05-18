#!/usr/bin/env python3.7
# ---------------------------------------------------------------------------------------------------#
# /Users/klluzbet/apache-spark/21-generate-data-ddl.py
# https://faker.readthedocs.io/en/master/
# ---------------------------------------------------------------------------------------------------#
from __future__ import print_function
# import findspark
# findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
#from pyspark.sql import SQLContext

from faker import Faker
import pprint
import random, os, sys, pyspark
from datetime import timedelta, timezone, datetime

import hashlib, re, yaml
field_name = []
slices = []
pp = pprint.PrettyPrinter(width=41, compact=True)
# ---------------------------------------------------------------------------------------------------#
s3location = "s3a://luzbetak/parquet-24"
current_slice = "0" 
total_slice = 1000
# ---------------------------------------------------------------------------------------------------#
access_key = os.environ.get('ACCESS_KEY')
secret_key = os.environ.get('SECRET_KEY')


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
def load_slice_distribution(filename):

    global slices

    with open(filename) as f:
        for line in f:
            line = line.rstrip("\n\r").split("|")
            # print(line[2])
            slices.append([ int(line[1]), int(line[2])]) 


# ---------------------------------------------------------------------------------------------------#
def get_field_names(fields):

    global field_name
    for rec in fields:
        array = rec.split('|')
        print(array[0])
        field_name.append(array[0])
# ---------------------------------------------------------------------------------------------------#
# Prototype to generate sample data: 
#    1. Generate client synthetic data into a list by appending the list
#    2. Parallelize list into Resilient Distributed Datasets (RDD)
#    3. Create DataFrame from the RDD
#    4. Save the DataFrame incrementally into S3 parquet format
#    5. Repeat 100 million times from 100+ servers in parallel 
#    6. Copy parquet format into Redshift Cluster
# ---------------------------------------------------------------------------------------------------#
# https://spark.apache.org/docs/latest/configuration.html
# ---------------------------------------------------------------------------------------------------#
def generate_sample_data(records):

    # os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.7"
    os.environ["SPARK_HOME"] = "/usr/local/spark-2.4.4-bin-hadoop2.7/"
    # os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.7 pyspark-shell"

    # --- Create Spark Session, Context and SQL ---#
    # spark = SparkSession.builder.appName("DataGenerator").config("spark.sql.crossJoin.enabled", "true").getOrCreate()
    
    # warehouse_location = abspath('warehouse')
    # .config("spark.sql.warehouse.dir", warehouse_location) \
    spark = SparkSession \
        .builder \
        .appName("DataGenerator") \
        .enableHiveSupport() \
        .getOrCreate()

    sc = spark.sparkContext
    #sc.setSystemProperty("spark.python.worker.memory", "4g")
    #sc.setSystemProperty("spark.driver.cores", "4")
    #sc.setSystemProperty("spark.driver.memory", "4g")
    #sc.setSystemProperty("spark.executor.memory", "2g")
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

    # --- Dynamically build set of records --- #
    faker = Faker()

    #for slice1 in slices:

    list1 = []  # list1 = [('0', '0', 0, 0), ('0', '0', 0, 0)]
    # print("******* Total Slices: " + str(slice1[1]) + " ***********")
    # for counter1 in range(0, 959):
    for counter1 in range(1, total_slice):
    # for counter1 in range(1, int(slice1[1])):

        temp = []
        for rec in records:

            array = rec.split('|')

            if array[1] == "BIGINT":
                temp.append(random.randint(100, 1_000))

            elif array[1] == "BOOLEAN":
                temp.append(random.choice([True, False]))

            elif array[1] == "DATE":
                temp.append(faker.date_between(start_date='-10y', end_date='now'))

            elif array[1] == "DATETIME":
                temp.append(faker.date_time_between(start_date='-10y', end_date='now'))

            else:
                # ------------ Primary Key ----------- #
                if re.search('sob', array[0]):
                    # temp.append(str(slice1[0]))
                    temp.append(current_slice)
                # ------------------------------------ #
                elif re.search('(id|ID)', array[0]):
                    temp.append(get_fake_id(random_sentence(1)))
                elif re.search('(name|family)', array[0]):
                    temp.append(faker.name())
                else:
                    temp.append(random_sentence(1))

        list1.append(temp)

        # if ((list1) and ((counter1 % 100) == 0)):
        if (counter1 % 100000) == 0:

            rdd = sc.parallelize(list1, 16)
            print("-" * 100)
            print("| RDD COUNT  : {}".format(rdd.count()))
            print("| field_name : {}".format(len(field_name)))
            print("-" * 100)

            # df = spark.createDataFrame(rdd, ['field1', 'field2', 'field3', 'field4'])
            df = spark.createDataFrame(rdd, field_name)
            df.printSchema()
            # df.collect()
            # df.show(25, False)

            # --- Write Parquet to S3 --- #
            # df.coalesce(1).write.parquet(s3location, mode="append") 
            # df.write.option("maxRecordsPerFile", 10000).save(s3location)
            # df.write.parquet(s3location, mode="overwrite")
            df.write.parquet(s3location, mode="append")

            # df.coalesce(1).write.json("kevin_01", mode="append")
            # df.write.partitionBy("sob").parquet(filename, mode="append")  
            # df.saveAsTextFile("hdfs://file.txt")
            # df.write.partitionBy("sob").json("kevin_02", mode="append")
            # df.write.partitionBy("sob","utc_date").json("kevin_005", mode="append")
            # df.map(lambda x: Row(var1 = x)).toDF().write.mode("append").saveAsTable("kevin_001")

            list1 = []  # list1 = [('0', '0', 0, 0), ('0', '0', 0, 0)]

    # ----------- Read Parquet from S3 ------------------ #
    # data = spark.read.parquet(filename)
    # data.show(25, False)
    # ------------ End of the Chunk Generation ---------- #

    # print(sc._conf.getAll())
    SparkContext.stop(sc)
    # spark.close()

# ---------------------------------------------------------------------------------------------------#
if __name__ == "__main__":

    print("Paramters for: {} | {}".format(sys.argv[0], sys.argv[1]))
    current_slice = sys.argv[1]
    total_slice = int(sys.argv[2])
    s3location = sys.argv[3]

    load_slice_distribution("90-ddl/3-slices.txt")
    fields = load_yaml_ddl("90-ddl/02-test-ddl.yaml")
    get_field_names(fields)
    generate_sample_data(fields)

# ---------------------------------------------------------------------------------------------------#
