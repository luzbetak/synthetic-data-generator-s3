#!/bin/bash
#----------------------------------------------------------------------------------#
#  --num-executors   = total-cores-in-cluster
#                    = num-cores-per-node * total-nodes-in-cluster 
#                    = 16 x 4 = 64
#  --executor-cores  = 4 executors per core
#  --executor-memory = amount of memory per executor
#                    = mem-per-node / num-executors-per-node
#                    = 64GB / 16 = 4GB
#----------------------------------------------------------------------------------#
killall python3.7
rm -f nohup.out
stop-all.sh; sleep 9; start-all.sh; sleep 9; 

#----------------------------------------------------------------------------------#
# spark-submit                                                              \
#     --master local[12]                                                    \
#     --packages org.apache.hadoop:hadoop-aws:2.7.7                         \
#     --driver-memory 32G                                                   \
#     --num-executors  3                                                    \
#     --executor-cores  4                                                   \
#     --executor-memory 8G                                                  \
#     2-read-orc-files.py                                                   \
#     "s3a://oath-nested-testing/fact_webclick_event/datehour=2019110500/"  \
#     "s3a://oath-nested-testing/fact_webclick_event/output-28/"            \
#     10000
#----------------------------------------------------------------------------------#


#----------------------------------------------------------------------------------#
# Total Servers:    4
# Total Cores:      16 * 4 = 64
# Total Memory:     64 * 4 = 256
# num-executors   = Total Cores - (number of server * 1)
#----------------------------------------------------------------------------------#
for i in {31..40}
do
    n=`printf %03d $i`
    echo "Slice: $n"
    
    # --master spark://172.31.44.181:7077               \
    nohup spark-submit                                  \
        --master local[2]                               \
        --packages org.apache.hadoop:hadoop-aws:2.7.7   \
        --driver-memory   4G                            \
        --num-executors   1                             \
        --executor-cores  2                             \
        --executor-memory 2G                            \
        21-generate-data.py                             \
        "$i"                                            \
        "10000000"                                      \
        "s3a://luzbetak/table_001/slice-$n" &
        sleep 3 
done
#----------------------------------------------------------------------------------#
# aws s3 ls s3://oath-nested-testing/fact_webclick_event/datehour=2019110500/
#----------------------------------------------------------------------------------#
