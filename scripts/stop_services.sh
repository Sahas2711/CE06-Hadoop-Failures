#!/usr/bin/env bash
# CE-06 Hadoop Practical - Stop all services (idempotent, safe to rerun)
# Stops: JobHistoryServer, NodeManager, ResourceManager, DataNode, NameNode
set -uo pipefail

HADOOP_HOME_DIR="/opt/hadoop-3.4.2"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=$HADOOP_HOME_DIR
export HADOOP_CONF_DIR=$HADOOP_HOME_DIR/etc/hadoop
export PATH=$PATH:$HADOOP_HOME_DIR/bin:$HADOOP_HOME_DIR/sbin

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: run as root inside WSL" >&2; exit 1
fi

echo "== Stopping JobHistoryServer =="
mapred --daemon stop historyserver 2>/dev/null && echo "  stopped" || echo "  not running"

echo "== Stopping NodeManager =="
yarn --daemon stop nodemanager 2>/dev/null && echo "  stopped" || echo "  not running"

echo "== Stopping ResourceManager =="
yarn --daemon stop resourcemanager 2>/dev/null && echo "  stopped" || echo "  not running"

echo "== Stopping DataNode =="
hdfs --daemon stop datanode 2>/dev/null && echo "  stopped" || echo "  not running"

echo "== Stopping NameNode =="
hdfs --daemon stop namenode 2>/dev/null && echo "  stopped" || echo "  not running"

echo "== Remaining Hadoop java processes =="
jps 2>/dev/null | grep -Ei 'NameNode|DataNode|ResourceManager|NodeManager|JobHistory' || echo "  none"
echo "All services stopped."
