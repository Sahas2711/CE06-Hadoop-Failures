#!/usr/bin/env bash
# CE-06 Hadoop Practical - Start all services (idempotent)
# Starts: NameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer
# No SSH required: each daemon started directly.
set -uo pipefail

HADOOP_HOME_DIR="/opt/hadoop-3.4.2"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=$HADOOP_HOME_DIR
export HADOOP_CONF_DIR=$HADOOP_HOME_DIR/etc/hadoop
export PATH=$PATH:$HADOOP_HOME_DIR/bin:$HADOOP_HOME_DIR/sbin

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: run as root inside WSL" >&2; exit 1
fi

echo "== Starting HDFS =="
if pgrep -f 'org.apache.hadoop.hdfs.server.namenode.NameNode' >/dev/null; then
  echo "  NameNode already running"
else
  setsid --fork hdfs --daemon start namenode && echo "  NameNode started"
fi
sleep 2
if pgrep -f 'org.apache.hadoop.hdfs.server.datanode.DataNode' >/dev/null; then
  echo "  DataNode already running"
else
  setsid --fork hdfs --daemon start datanode && echo "  DataNode started"
fi

echo "== Waiting for DataNode to register with NameNode =="
for i in $(seq 1 30); do
  LIVE=$(hdfs dfsadmin -report 2>/dev/null | grep 'Live datanodes' | grep -o '[0-9]*' | head -1)
  if [ "${LIVE:-0}" -ge 1 ] 2>/dev/null; then
    echo "  live datanodes: $LIVE"
    break
  fi
  sleep 2
done

echo "== Starting YARN =="
if pgrep -f 'org.apache.hadoop.yarn.server.resourcemanager.ResourceManager' >/dev/null; then
  echo "  ResourceManager already running"
else
  setsid --fork yarn --daemon start resourcemanager && echo "  ResourceManager started"
fi
sleep 2
if pgrep -f 'org.apache.hadoop.yarn.server.nodemanager.NodeManager' >/dev/null; then
  echo "  NodeManager already running"
else
  setsid --fork yarn --daemon start nodemanager && echo "  NodeManager started"
fi

echo "== Starting JobHistoryServer =="
if pgrep -f 'org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer' >/dev/null; then
  echo "  JobHistoryServer already running"
else
  setsid --fork mapred --daemon start historyserver && echo "  JobHistoryServer started"
fi

echo "== Service status =="
jps 2>/dev/null || true

echo ""
echo "UIs (open in Windows browser):"
echo "  NameNode UI          http://localhost:9870"
echo "  ResourceManager UI   http://localhost:8088"
echo "  NodeManager UI       http://localhost:8042"
echo "  JobHistory UI        http://localhost:19888"
