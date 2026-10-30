#!/bin/bash
# CE-06: verify copy completeness + hadoop binary works
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
SRC=/mnt/c/bigdata/hadoop-3.4.2
DST=/opt/hadoop-3.4.2
sn=$(find "$SRC" -type f -not -path '*/share/doc/*' | wc -l)
dn=$(find "$DST" -type f -not -path '*/share/doc/*' | wc -l)
echo "non-doc files: src=$sn wsl=$dn"
if [ "$sn" -ne "$dn" ]; then
  echo "MISSING (non-doc):"
  (cd "$SRC" && find . -type f -not -path './share/doc/*' | sort) > /tmp/s.lst
  (cd "$DST" && find . -type f -not -path './share/doc/*' | sort) > /tmp/d.lst
  comm -23 /tmp/s.lst /tmp/d.lst | head -30
  comm -23 /tmp/s.lst /tmp/d.lst | wc -l
fi
echo "--- hadoop version ---"
"$DST/bin/hadoop" version 2>&1 | head -3
