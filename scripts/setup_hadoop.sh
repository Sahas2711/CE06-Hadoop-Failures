#!/usr/bin/env bash
# =============================================================================
# CE-06 Hadoop Practical - Pseudo-Distributed Setup (WSL2 Ubuntu)
# Student: NAGAR SAHAS PRAKASH (Roll 202301040048)
#
# Idempotent: safe to rerun. Existing configs are backed up before overwrite.
# Namenode is formatted ONLY if its data directory is empty.
#
# Usage (as root inside WSL):
#   bash setup_hadoop.sh
# =============================================================================
set -euo pipefail

HADOOP_HOME_DIR="/opt/hadoop-3.4.2"
JAVA_HOME_DIR="/usr/lib/jvm/java-17-openjdk-amd64"
CONF_DIR="$HADOOP_HOME_DIR/etc/hadoop"
DATA_ROOT="/opt/hadoop-data"
BACKUP_DIR="$DATA_ROOT/config-backups"
STAMP="$(date +%Y%m%d_%H%M%S)"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: run as root (wsl -d Ubuntu -u root -- bash setup_hadoop.sh)" >&2
  exit 1
fi

echo "== [1/8] Verifying prerequisites =="
export JAVA_HOME="$JAVA_HOME_DIR"
export HADOOP_HOME="$HADOOP_HOME_DIR"
export HADOOP_CONF_DIR="$CONF_DIR"
[ -x "$JAVA_HOME_DIR/bin/java" ]  || { echo "ERROR: JDK missing at $JAVA_HOME_DIR"; exit 1; }
[ -x "$HADOOP_HOME_DIR/bin/hadoop" ] || { echo "ERROR: Hadoop missing at $HADOOP_HOME_DIR"; exit 1; }
echo "  java:  $($JAVA_HOME_DIR/bin/java -version 2>&1 | head -1)"
echo "  hadoop: $($HADOOP_HOME_DIR/bin/hadoop version 2>&1 | head -1)"

echo "== [2/8] Backing up existing configs (only if present) =="
mkdir -p "$BACKUP_DIR"
for f in core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml hadoop-env.sh log4j.properties; do
  if [ -f "$CONF_DIR/$f" ] && ! grep -q "CE06-PRACTICAL" "$CONF_DIR/$f" 2>/dev/null; then
    cp -n "$CONF_DIR/$f" "$BACKUP_DIR/$f.$STAMP.bak" && echo "  backed up $f"
  fi
done

echo "== [3/8] Writing configuration files =="
cp /dev/stdin "$CONF_DIR/core-site.xml" <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<!-- CE06-PRACTICAL core-site -->
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/opt/hadoop-data/tmp</value>
  </property>
</configuration>
XML

cp /dev/stdin "$CONF_DIR/hdfs-site.xml" <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<!-- CE06-PRACTICAL hdfs-site -->
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/opt/hadoop-data/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/opt/hadoop-data/datanode</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
  </property>
</configuration>
XML

cp /dev/stdin "$CONF_DIR/mapred-site.xml" <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<!-- CE06-PRACTICAL mapred-site -->
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.address</name>
    <value>localhost:10020</value>
  </property>
  <property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>localhost:19888</value>
  </property>
  <property>
    <name>mapreduce.map.maxattempts</name>
    <value>4</value>
  </property>
  <property>
    <name>mapreduce.reduce.maxattempts</name>
    <value>4</value>
  </property>
  <property>
    <name>mapreduce.map.speculative</name>
    <value>true</value>
  </property>
  <property>
    <name>mapreduce.reduce.speculative</name>
    <value>false</value>
  </property>
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>512</value>
  </property>
  <property>
    <name>mapreduce.reduce.memory.mb</name>
    <value>512</value>
  </property>
  <property>
    <name>mapreduce.map.java.opts</name>
    <value>-Xmx400m</value>
  </property>
  <property>
    <name>mapreduce.reduce.java.opts</name>
    <value>-Xmx400m</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>HADOOP_MAPRED_HOME=/opt/hadoop-3.4.2,HADOOP_COMMON_HOME=/opt/hadoop-3.4.2,HADOOP_HDFS_HOME=/opt/hadoop-3.4.2,HADOOP_YARN_HOME=/opt/hadoop-3.4.2</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>HADOOP_MAPRED_HOME=/opt/hadoop-3.4.2</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>HADOOP_MAPRED_HOME=/opt/hadoop-3.4.2</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.resource.mb</name>
    <value>512</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.java.opts</name>
    <value>-Xmx400m</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>/opt/hadoop-3.4.2/share/hadoop/mapreduce/*,/opt/hadoop-3.4.2/share/hadoop/mapreduce/lib/*</value>
  </property>
  <property>
    <name>mapreduce.input.fileinputformat.split.maxsize</name>
    <value>33554432</value>
  </property>
</configuration>
XML

cp /dev/stdin "$CONF_DIR/yarn-site.xml" <<'XML'
<?xml version="1.0" encoding="UTF-8"?>
<!-- CE06-PRACTICAL yarn-site -->
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>localhost</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>2048</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>128</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>1536</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.log-aggregation-enable</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.nodemanager.liveness-monitor.expiry-interval-secs</name>
    <value>30</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
  </property>
</configuration>
XML

# hadoop-env.sh: ensure JAVA_HOME set (append marker block only once)
if ! grep -q "CE06-PRACTICAL" "$CONF_DIR/hadoop-env.sh"; then
  cat >> "$CONF_DIR/hadoop-env.sh" <<'ENV'

# CE06-PRACTICAL
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=/opt/hadoop-3.4.2/etc/hadoop
export HADOOP_COMMON_HOME=/opt/hadoop-3.4.2
export HADOOP_HDFS_HOME=/opt/hadoop-3.4.2
export HADOOP_YARN_HOME=/opt/hadoop-3.4.2
export HADOOP_MAPRED_HOME=/opt/hadoop-3.4.2
export HDFS_NAMENODE_OPTS="-Xmx256m"
export HDFS_DATANODE_OPTS="-Xmx256m"
export HADOOP_JOB_HISTORYSERVER_HEAPSIZE=256
export YARN_RESOURCEMANAGER_HEAPSIZE=256
export YARN_NODEMANAGER_HEAPSIZE=256
export HADOOP_SHELL_EXECJAVA=/usr/lib/jvm/java-17-openjdk-amd64/bin/java
ENV
  echo "  appended CE06 block to hadoop-env.sh"
fi

# capacity-scheduler: allow AM up to 50% of NM memory (default 10% = 204MB < 512MB AM)
cat > /tmp/_fix_amcap.py <<'PYEOF'
import re, sys
p = sys.argv[1]
s = open(p).read()
s2 = re.sub(r'(<name>yarn\.scheduler\.capacity\.maximum-am-resource-percent</name>\s*<value>)[0-9.]+(</value>)',
            r'\g<1>0.5\g<2>', s)
if s2 != s:
    open(p, 'w').write(s2)
    print("  capacity-scheduler: maximum-am-resource-percent -> 0.5")
else:
    print("  capacity-scheduler: maximum-am-resource-percent already 0.5 or key absent (check manually)")
PYEOF
python3 /tmp/_fix_amcap.py "$CONF_DIR/capacity-scheduler.xml"

echo "== [4/8] Environment profile for all users =="
cp /dev/stdin /etc/profile.d/ce06-hadoop.sh <<'ENV'
# CE06-PRACTICAL
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=/opt/hadoop-3.4.2/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV
chmod 644 /etc/profile.d/ce06-hadoop.sh

echo "== [5/8] Data directories =="
mkdir -p "$DATA_ROOT/tmp" "$DATA_ROOT/namenode" "$DATA_ROOT/datanode" \
         "$DATA_ROOT/hdfs_staging" "$DATA_ROOT/user"
chmod -R 777 "$DATA_ROOT"

echo "== [6/8] Format namenode (ONLY if not already formatted) =="
if [ -f "$DATA_ROOT/namenode/current/VERSION" ]; then
  echo "  namenode already formatted - skipping"
else
  "$HADOOP_HOME_DIR/bin/hdfs" namenode -format -force -nonInteractive > \
    "$DATA_ROOT/format_namenode_${STAMP}.log" 2>&1
  echo "  formatted (log: $DATA_ROOT/format_namenode_${STAMP}.log)"
fi

echo "== [7/8] Hadoop configuration self-test (verifies files Hadoop reads) =="
echo "  fs.defaultFS              = $("$HADOOP_HOME_DIR/bin/hdfs" getconf -confKey fs.defaultFS)"
echo "  dfs.replication           = $("$HADOOP_HOME_DIR/bin/hdfs" getconf -confKey dfs.replication)"
echo "  --- mapred-site.xml effective values (also visible in JobHistory jobconf page) ---"
for k in mapreduce.framework.name mapreduce.map.maxattempts mapreduce.reduce.maxattempts \
         mapreduce.map.speculative mapreduce.reduce.speculative \
         mapreduce.jobhistory.webapp.address mapreduce.input.fileinputformat.split.maxsize; do
  v=$(grep -A1 "<name>$k</name>" "$CONF_DIR/mapred-site.xml" | grep '<value>' | sed 's/.*<value>\(.*\)<\/value>.*/\1/')
  echo "  $k = ${v:-<not set - using default>}"
done
echo "  --- yarn-site.xml effective values ---"
for k in yarn.nodemanager.resource.memory-mb yarn.scheduler.minimum-allocation-mb \
         yarn.nodemanager.liveness-monitor.expiry-interval-secs; do
  v=$(grep -A1 "<name>$k</name>" "$CONF_DIR/yarn-site.xml" | grep '<value>' | sed 's/.*<value>\(.*\)<\/value>.*/\1/')
  echo "  $k = ${v:-<not set - using default>}"
done
echo "  --- capacity-scheduler.xml ---"
v=$(grep -A1 'maximum-am-resource-percent' "$CONF_DIR/capacity-scheduler.xml" | grep '<value>' | sed 's/.*<value>\(.*\)<\/value>.*/\1/')
echo "  yarn.scheduler.capacity.maximum-am-resource-percent = $v"
echo "  NOTE: JobHistory UI job-conf page shows the same values as loaded by the job at runtime."

echo "== [8/8] Setup complete =="
echo "Next: bash start_services.sh"
