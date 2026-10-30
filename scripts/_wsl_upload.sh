#!/bin/bash
# CE-06: upload prepared datasets to HDFS (idempotent with -put -f)
set -e
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin

PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical

hdfs dfs -mkdir -p /data/clean /data/malformed /results /tmp/ce06
echo "Uploading clean dataset (373 MB)..."
hdfs dfs -put -f "$PROJ/dataset/clean/nasa_access_clean.log" /data/clean/
echo "Uploading malformed dataset (377 MB)..."
hdfs dfs -put -f "$PROJ/dataset/malformed/nasa_access_malformed.log" /data/malformed/
echo "--- HDFS listing ---"
hdfs dfs -ls -h /data/clean /data/malformed
echo "UPLOAD_OK"
