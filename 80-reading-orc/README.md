Apache Spark Hints
===================

### Configuration
```
conf = pyspark.SparkConf()
conf.setMaster('spark://head_node:56887')
conf.set('spark.authenticate', True)
conf.set('spark.authenticate.secret', 'secret-key')
sc = SparkContext(conf=conf)

```

### Create Spark
```
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Learning_Spark") \
    .getOrCreate()
```

### Read Data
```
data = spark.read.csv('Video_Games_Sales.csv',inferSchema=True, header=True)
```

### Display Data
```
data.printSchema()
data.show(5)
```

### Select Data
```
data.select("Name","Platform","User_Score","User_Count").show(15, truncate=False)
```

### Data Aggregation
```
data.groupBy("Platform") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10)
```

### References:
* [https://spark.apache.org/docs/2.4.4/submitting-applications.html](https://spark.apache.org/docs/2.4.4/submitting-applications.html)
