#!/bin/bash
# Wait for application_1793346852065_0007 (exp05) to finish, then capture evidence.
set -u
PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin

APP=application_1793346852065_0007
JOB=job_1793346852065_0007
LOG=$PROJ/evidence/logs/exp05_spec_on_run.log
JH=$PROJ/evidence/job_history
OUT=/results/exp05_spec_on
mkdir -p "$JH" "$PROJ/evidence/logs"

echo "=== EXP05 (spec ON) resumed evidence capture $(date) ===" | tee "$LOG"
echo "Waiting for $APP to finish..." | tee -a "$LOG"
while true; do
  STATE=$(curl -s "http://localhost:8088/ws/v1/cluster/apps/$APP" \
    | python3 -c "import sys,json;a=json.load(sys.stdin)['app'];print(a['state'],a['finalStatus'])" 2>/dev/null)
  echo "  $STATE $(date +%T)" | tee -a "$LOG"
  case "$STATE" in
    FINISHED*|FAILED*|KILLED*) break;;
  esac
  sleep 10
done
echo "Final: $STATE" | tee -a "$LOG"

sleep 5
{
  echo "########## JOB HISTORY EVIDENCE: $JOB (exp 05 spec ON) ##########"
  mapred job -history "$OUT" 2>&1 || true
  echo "----- yarn application -status -----"
  yarn application -status "$APP" 2>&1 || true
  echo "----- counters -----"
  mapred job -counter "$JOB" 'Job Counters' 2>&1 || true
  mapred job -counter "$JOB" 'CE06 Failure Simulation' 2>&1 || true
  echo "----- REST job detail -----"
  curl -s "http://localhost:19888/ws/v1/history/mapreduce/jobs/$JOB" || true
  echo
} > "$JH/exp05_jobhistory_$JOB.txt" 2>&1

curl -s "http://localhost:19888/ws/v1/history/mapreduce/jobs/$JOB/tasks" \
  > "$JH/exp05_tasks_$JOB.json" || true

echo "--- task attempts per task (key evidence: 000000 has 2 attempts?) ---" | tee -a "$LOG"
python3 - <<'EOF' | tee -a "$LOG"
import json
d = json.load(open('/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/job_history/exp05_tasks_job_1793346852065_0007.json'))
tasks = d['tasks']['task']
for t in tasks:
    if t['type'] != 'MAP':
        continue
    det = json.load(open('/dev/stdin')) if False else None
# fetch attempts per task
import urllib.request
base = 'http://localhost:19888/ws/v1/history/mapreduce/jobs/job_1793346852065_0007/tasks'
for t in tasks:
    tid = t['id']
    if t['type'] != 'MAP':
        continue
    with urllib.request.urlopen(f'{base}/{tid}/taskattempts') as r:
        att = json.load(r)['taskAttempts']['taskAttempt']
    states = [a['status'] for a in sorted(att, key=lambda x: x['id'])]
    ids = [a['id'].split('_')[-2:] for a in sorted(att, key=lambda x: x['id'])]
    print(tid, t['status'], '-> attempts:', list(zip(
        ['_'.join(x) for x in ids], states)),
        'COUNT=%d' % len(att))
EOF

echo "--- output sample ---" | tee -a "$LOG"
hdfs dfs -cat "$OUT/part-r-*" 2>/dev/null | tee -a "$LOG"
echo "RESULT=$([ "$STATE" = 'FINISHED SUCCEEDED' ] && echo SUCCESS || echo FAILED) EXP=05 JOBID=$JOB" | tee -a "$LOG"
echo "EXP05 evidence done."
