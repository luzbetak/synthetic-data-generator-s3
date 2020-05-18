# Run a Python application on a Spark standalone cluster
spark-submit \
  --master spark://172.31.9.145:7077 \
    pi.py \
    1000

