#!/usr/bin/env python
# ------------------------------------------------------------------------- #
# Reading ORC files
# ------------------------------------------------------------------------- #
from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os, sys

# ------------------------------------------------------------------------- #
if __name__ == "__main__":

    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    spark = SparkSession.builder.appName("ReadORC").getOrCreate()
    # sc = SparkContext("local[8]", "Read ORC Files")
    sc = spark.sparkContext

    # --- Amazon S3 Configuration --- #
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")

    # sqlContext = SQLContext(sc)
    # df = sqlContext.read.format("orc").load("s3://oath-nested-testing/fact_webclick_event/datehour=2019110500/")
    # spark = SparkSession(sc)

    # LoadOrc = spark.read.option("inferSchema", True).orc(sys.argv[1])
    df = spark.read.option("inferSchema", True).orc(sys.argv[1])
    df.printSchema()

    # dt.show(10)
    # dt.write.orc("s3a://oath-nested-testing/fact_webclick_event/output-12/").coalesce(10000)
    # val df = spark.range(100).coalesce(1)
    df.write.option("maxRecordsPerFile", int(sys.argv[3])) #.save(sys.argv[2])
    df.write.mode("overwrite").parquet(sys.argv[2])

    sc.stop()

