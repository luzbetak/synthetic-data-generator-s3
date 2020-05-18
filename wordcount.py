#!/usr/bin/env python
#-------------------------------------------------------------------------#
## wordcount.py
#-------------------------------------------------------------------------#
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark import SparkContext
import os, sys 

access_key = os.environ.get('ACCESS_KEY')
secret_key = os.environ.get('SECRET_KEY')
#-------------------------------------------------------------------------#
if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: wordcount  ", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="WordCount")

    # --- Amazon S3 Configuration ---#
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")

    text_file = sc.textFile(sys.argv[1])
    counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    print("*" * 80)
    print("Type: " +  str(type(counts)))
    print("*" * 80)


    spark = SparkSession(sc)
    hasattr(counts, "toDF")
    #df = counts.toDF()
    df = counts.map(lambda x : (x[0], x[1])).toDF(("keyword", "total"))
    df.show()

    # counts.saveAsTextFile(sys.argv[2])
    df.write.mode('overwrite').json(sys.argv[2]) 
    sc.stop()


