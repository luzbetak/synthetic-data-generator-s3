#!/bin/bash
#----------------------------------------------------------------------------------#
spark-submit                                            \
  --class --packages org.apache.hadoop:hadoop-aws:2.7.7 \
  --master spark://172.31.9.145:7077                    \
  --executor-memory 16G                                 \
  --total-executor-cores 50                             \
  /root/40-apache-spark/pi.py                           \
  1000

#----------------------------------------------------------------------------------#
# spark-submit                                                                \
#   --driver-memory   16G                                                     \
#   --num-executors   4                                                       \
#   --executor-cores  4                                                       \
#   --executor-memory 4G                                                      \
#   --master spark://172.31.9.145:7077                                        \
#   21-generate-data.py
#----------------------------------------------------------------------------------#

