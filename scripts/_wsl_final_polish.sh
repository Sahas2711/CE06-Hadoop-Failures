#!/bin/bash
# Final polish: label jobs 0010/0011 in summary + future collect runs, verify cluster.
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin

SUM=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/REPORT_EVIDENCE_SUMMARY.txt
COL=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/scripts/collect_evidence.sh

sed -i 's/0010   CE06-SLOW_TASK-1793350497885/0010   EXP4v2 SLOW_TASK spec OFF (sleep=400 final pair)/' "$SUM"
sed -i 's/0011   CE06-SLOW_TASK-1793351816193/0011   EXP5v2 SLOW_TASK spec ON sleep=400 (duplicate proof)/' "$SUM"
grep -q "0010.*EXP4v2" "$COL" || sed -i "s/'0007':'EXP5  SLOW_TASK spec ON',/'0007':'EXP5  SLOW_TASK spec ON (prelim sleep=150)', '0010':'EXP4v2 spec OFF sleep=400', '0011':'EXP5v2 spec ON sleep=400',/" "$COL"

echo "=== labeled rows ==="
grep -E '^00(0[1-9]|1[01]) ' "$SUM"
echo
echo "=== cluster ==="
jps 2>/dev/null | grep -v Jps | sort
yarn node -list -all 2>/dev/null | tail -n +3
echo "=== /results ==="
hdfs dfs -ls /results 2>/dev/null | awk '{print $NF}'
echo "=== deliverables ==="
ls /mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/*.md /mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/src/*.md /mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/src/build/*.jar
echo "evidence files: $(find /mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence -type f | wc -l)"
