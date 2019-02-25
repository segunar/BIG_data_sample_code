#!/bin/sh
#
###################################################################################
#
# 1. LOAD THE FILES FROM THE FILE SYSTEM TO HADOOP DISTRIBUTED FILE SYSTEM (HDFS)
#
###################################################################################
#
hadoop fs -put my_dataset /user/cloudera/my_dataset
#
###################################################################################
#
# 2. TRIGGER THE MAPREDUCE JOB
#
###################################################################################
#
hadoop jar \
/usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-input /user/cloudera/my_dataset \
-output /user/cloudera/my_result \
-file ~/my_python_mapreduce/my_mapper.py \
-file ~/my_python_mapreduce/my_reducer.py \
-mapper ~/my_python_mapreduce/my_mapper.py \
-reducer ~/my_python_mapreduce/my_reducer.py
#
###################################################################################
#
# 3. BRING THE RESULT FOLDER BACK TO THE FILE SYSTEM
#
###################################################################################
#
hadoop fs -get my_result ~/my_result
#
###################################################################################
#
# 4. REMOVE THE FOLDERS FROM HDFS 
#
###################################################################################
#
hadoop fs -rmr my_dataset
hadoop fs -rmr my_result
#
