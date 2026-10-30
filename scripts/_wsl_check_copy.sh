#!/bin/bash
# Helper: verify the hadoop copy integrity (CE-06)
SRC=/mnt/c/bigdata/hadoop-3.4.2
DST=/opt/hadoop-3.4.2
for f in share/hadoop/common/hadoop-common-3.4.2.jar \
         share/hadoop/hdfs/hadoop-hdfs-3.4.2.jar \
         share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.4.2.jar \
         share/hadoop/mapreduce/hadoop-mapreduce-examples-3.4.2.jar \
         share/hadoop/yarn/hadoop-yarn-client-3.4.2.jar \
         sbin/start-dfs.sh libexec/hadoop-functions.sh bin/hadoop; do
  if [ -f "$DST/$f" ]; then echo "OK $f"; else echo "MISSING $f"; fi
done
echo "wsl_share_files=$(find $DST/share/hadoop -type f 2>/dev/null | wc -l)"
echo "src_share_files=$(find $SRC/share/hadoop -type f | wc -l)"
echo "wsl_total_files=$(find $DST -type f 2>/dev/null | wc -l)"
echo "src_total_files=$(find $SRC -type f | wc -l)"
