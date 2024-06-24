#!/bin/bash

hdfs dfs -mkdir /data/
hdfs dfs -D dfs.block.size=32M -put /train.csv /data/
hdfs dfsadmin -setSpaceQuota 5g /
exit