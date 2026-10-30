#!/bin/bash
# =============================================================================
# EXP 6: HUMAN/PROCESS-LEVEL FAILURE - kill -9 a running task-attempt JVM.
# Job: SLOW_TASK (straggler on partition 0, attempt _0 sleeps 250ms/1000 lines
#  => ~70s runtime, enough window to kill). attempt _1 (retry) runs fast.
# Expected: attempt_..._m_000000_0 FAILED (killed -9), attempt_..._m_000000_1
#  SUCCEEDED, job overall SUCCEEDED.
# =============================================================================
set -u
PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin

LOG=$PROJ/evidence/logs/exp06_kill_attempt_run.log
JH=$PROJ/evidence/job_history
JAR=$PROJ/src/build/FailureSimulation.jar
INPUT=/data/clean/nasa_access_clean.log
OUT=/results/exp06_kill_attempt
mkdir -p "$JH"

say() { echo "$@" | tee -a "$LOG"; }
hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null 2>&1

say "=== EXP 06 | kill -9 of running map attempt JVM | $(date) ==="
START=$(date +%s)

# --- submit job in background (detached so it survives shell hiccups) ---
setsid --fork hadoop jar "$JAR" FailureSimulation \
  -Dce06.simulation.mode=SLOW_TASK -Dce06.sleep.ms=250 \
  -Dmapreduce.map.speculative=false \
  SLOW_TASK "$INPUT" "$OUT" > "$PROJ/evidence/logs/exp06_client.log" 2>&1
say "Job submitted (client log: evidence/logs/exp06_client.log)"

# --- find APP/JOB id from RM ---
APP=""; JOB=""
for i in $(seq 1 30); do
  APP=$(curl -s 'http://localhost:8088/ws/v1/cluster/apps?states=RUNNING' 2>/dev/null \
        | python3 -c "import sys,json;a=json.load(sys.stdin).get('apps',{}).get('app') or [];print(a[-1]['id'] if a else '')" 2>/dev/null)
  [ -n "$APP" ] && break
  sleep 2
done
JOB=$(echo "$APP" | sed 's/application/job/')
say "APP=$APP JOB=$JOB"

# --- wait for the target attempt JVM (partition 0, attempt _0) ---
KILLED_PID=""
TARGET="attempt_[0-9]*_m_000000_0"
for i in $(seq 1 60); do
  PID=$(pgrep -f "mapred.YarnChild.*_m_000000_0" | head -1)
  if [ -z "$PID" ]; then
    PID=$(jps -l 2>/dev/null | awk '/YarnChild/ {print $1}' | while read p; do
      if tr '\0' ' ' < /proc/$p/cmdline 2>/dev/null | grep -q "_m_000000_0"; then echo $p; break; fi
    done)
  fi
  if [ -n "$PID" ]; then
    say "Found target attempt JVM PID=$PID at $(date +%T) (after ${i} polls)"
    say "cmdline: $(tr '\0' ' ' < /proc/$PID/cmdline | cut -c1-250)"
    # snapshot of all YarnChild JVMs before kill
    say "All YarnChild JVMs BEFORE kill:"
    jps -l 2>/dev/null | tee -a "$LOG"
    kill -9 "$PID" && KILLED_PID=$PID
    say "SENT kill -9 to PID $PID at $(date +%T)"
    sleep 2
    say "PID $PID alive after kill? $(kill -0 $PID 2>/dev/null && echo YES || echo NO)"
    break
  fi
  sleep 2
done
[ -z "$KILLED_PID" ] && say "WARNING: target JVM never found in 120s window"

# --- watch job until finished ---
say "Watching $APP until finish..."
while true; do
  ST=$(curl -s "http://localhost:8088/ws/v1/cluster/apps/$APP" 2>/dev/null \
       | python3 -c "import sys,json;a=json.load(sys.stdin)['app'];print(a['state'],a['finalStatus'])" 2>/dev/null)
  say "  state=$ST $(date +%T)"
  case "$ST" in FINISHED*|FAILED*|KILLED*) break;; esac
  sleep 10
done
END=$(date +%s)
say "FINAL: $ST DURATION=$((END-START))s"

sleep 5
# --- evidence ---
{
  echo "########## JOB HISTORY: $JOB (exp06 kill attempt) ##########"
  mapred job -history "$OUT" 2>&1 || true
  echo "----- application status -----"
  yarn application -status "$APP" 2>&1 || true
  echo "----- Job Counters -----"
  mapred job -counter "$JOB" 'Job Counters' 2>&1 || true
} > "$JH/exp06_jobhistory_$JOB.txt" 2>&1

curl -s "http://localhost:19888/ws/v1/history/mapreduce/jobs/$JOB/tasks" \
  > "$JH/exp06_tasks_$JOB.json" 2>/dev/null

say "--- task attempt states (expect task 000000: _0 FAILED + _1 SUCCEEDED) ---"
python3 - "$JOB" <<'EOF' 2>&1 | tee -a "$LOG"
import json, sys, urllib.request
job = sys.argv[1]
base = f'http://localhost:19888/ws/v1/history/mapreduce/jobs/{job}'
with urllib.request.urlopen(base + '/tasks') as r:
    tasks = json.load(r)['tasks']['task']
for t in tasks:
    if t['type'] != 'MAP':
        continue
    with urllib.request.urlopen(f"{base}/tasks/{t['id']}/attempts") as r:
        att = json.load(r)['taskAttempts'].get('taskAttempt') or []
    att = sorted(att, key=lambda x: x['id'])
    print(t['id'], t['state'],
          ' '.join(a['id'].rsplit('_',1)[-1] + '=' + a['state'] for a in att),
          f'(attempts={len(att)})')
EOF

say "--- yarn logs for the killed container (diagnostic) ---"
yarn logs -applicationId "$APP" 2>/dev/null | grep -E "Killed|SIGKILL|exit code|Container.*killed|attempt_.*_m_000000_0" | head -40 | tee -a "$LOG" || true

say "--- output sample ---"
hdfs dfs -cat "$OUT/part-r-*" 2>/dev/null | head -10 | tee -a "$LOG"
say "RESULT=$([ "$ST" = 'FINISHED SUCCEEDED' ] && echo SUCCESS || echo FAILED) EXP=06 JOBID=$JOB KILLED_PID=${KILLED_PID:-none}"
echo "EXP06 done."
