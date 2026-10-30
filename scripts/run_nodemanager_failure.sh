#!/bin/bash
# =============================================================================
# EXP 7: INFRASTRUCTURE FAILURE - stop NodeManager mid-job, restart later.
# Job: SLOW_TASK (partition-0 straggler ~70s) submitted detached.
# At ~30s: yarn --daemon stop nodemanager (safe stop).
#  -> RM stops seeing heartbeats -> after liveness expiry (30s, configured)
#     RM marks node LOST -> running containers (incl. possibly AM) fail.
# At ~90s: yarn --daemon start nodemanager -> node back.
#  -> if AM died: RM relaunches AM (max-attempts), MR AM recovers job state
#     from HDFS and re-runs unfinished tasks; if AM survived as orphan, it
#     reschedules failed task attempts on the revived node.
# Expected: job eventually SUCCEEDED (document whatever actually happens).
# =============================================================================
set -u
PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

LOG=$PROJ/evidence/logs/exp07_nodemanager_failure_run.log
JH=$PROJ/evidence/job_history
JAR=$PROJ/src/build/FailureSimulation.jar
INPUT=/data/clean/nasa_access_clean.log
OUT=/results/exp07_nodemanager_failure
mkdir -p "$JH"

say() { echo "$@" | tee -a "$LOG"; }
snap_nodes() { yarn node -list -all 2>/dev/null | tail -n +3 | tee -a "$LOG"; }

hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null 2>&1

say "=== EXP 07 | NodeManager failure mid-job | $(date) ==="
say "Nodes BEFORE:"
snap_nodes
START=$(date +%s)

setsid --fork hadoop jar "$JAR" FailureSimulation \
  -Dce06.simulation.mode=SLOW_TASK -Dce06.sleep.ms=250 \
  -Dmapreduce.map.speculative=false \
  -Dyarn.resourcemanager.am.max-attempts=3 \
  SLOW_TASK "$INPUT" "$OUT" > "$PROJ/evidence/logs/exp07_client.log" 2>&1
say "Job submitted (client log: evidence/logs/exp07_client.log)"

APP=""
for i in $(seq 1 30); do
  APP=$(curl -s 'http://localhost:8088/ws/v1/cluster/apps?states=RUNNING' 2>/dev/null \
        | python3 -c "import sys,json;a=json.load(sys.stdin).get('apps',{}).get('app') or [];print(a[-1]['id'] if a else '')" 2>/dev/null)
  [ -n "$APP" ] && break
  sleep 2
done
JOB=$(echo "$APP" | sed 's/application/job/')
say "APP=$APP JOB=$JOB"
say "AM / YarnChild processes BEFORE stop:"
jps -l 2>/dev/null | tee -a "$LOG"

# --- phase 1: let the job get going (attempt _0 busy ~30s) ---
say "Letting job run 30s before stopping NodeManager..."
for i in $(seq 6); do sleep 5; say "  t+$((i*5))s $(curl -s http://localhost:8088/ws/v1/cluster/apps/$APP | python3 -c "import sys,json;print(json.load(sys.stdin)['app'].get('progress'))" 2>/dev/null)%"; done

# --- phase 2: STOP the NodeManager (safe stop) ---
say ">>> yarn --daemon stop nodemanager  ($(date +%T))"
yarn --daemon stop nodemanager 2>&1 | tee -a "$LOG"
sleep 3
say "Processes AFTER stop:"
jps -l 2>/dev/null | tee -a "$LOG"
say "Nodes AFTER stop:"
snap_nodes

# --- phase 3: observe the blast radius for ~60s while node is down ---
say "Observing app state while NodeManager is DOWN..."
for i in $(seq 1 6); do
  sleep 10
  ST=$(curl -s "http://localhost:8088/ws/v1/cluster/apps/$APP" 2>/dev/null \
       | python3 -c "import sys,json;a=json.load(sys.stdin)['app'];print(a['state'],a['finalStatus'],'prog='+str(a.get('progress')))" 2>/dev/null)
  say "  t-down+$((i*10))s state=$ST $(date +%T)"
  [ "${ST%% *}" = "FINISHED" ] && break
done
say "Application log tail (AM/RM diagnostics) while down:"
curl -s "http://localhost:8088/ws/v1/cluster/apps/$APP" 2>/dev/null \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['app'].get('diagnostics','')[:1200])" 2>/dev/null | tee -a "$LOG"

# --- phase 4: restart the NodeManager ---
# NOTE: wrap in setsid --fork so the NM detaches from THIS shell session;
# otherwise it dies with the session when the script's wsl.exe exits.
say ">>> setsid --fork yarn --daemon start nodemanager  ($(date +%T))"
setsid --fork yarn --daemon start nodemanager 2>&1 | tee -a "$LOG"
sleep 10
say "Processes AFTER restart:"
jps -l 2>/dev/null | tee -a "$LOG"
say "Nodes AFTER restart:"
snap_nodes

# --- phase 5: wait for final app state ---
say "Waiting for final application state..."
ST=""
while true; do
  ST=$(curl -s "http://localhost:8088/ws/v1/cluster/apps/$APP" 2>/dev/null \
       | python3 -c "import sys,json;a=json.load(sys.stdin)['app'];print(a['state'],a['finalStatus'])" 2>/dev/null)
  say "  state=$ST $(date +%T)"
  case "$ST" in FINISHED*|FAILED*|KILLED*) break;; esac
  T=$(( $(date +%s) - START ))
  [ $T -gt 900 ] && { say "TIMEOUT after 15min"; break; }
  sleep 15
done
END=$(date +%s)
say "FINAL: $ST DURATION=$((END-START))s"

sleep 5
{
  echo "########## JOB HISTORY: $JOB (exp07 nodemanager failure) ##########"
  mapred job -history "$OUT" 2>&1 || true
  echo "----- application status -----"
  yarn application -status "$APP" 2>&1 || true
  echo "----- nodes final -----"
  yarn node -list -all 2>&1 || true
  echo "----- Job Counters -----"
  mapred job -counter "$JOB" 'Job Counters' 2>&1 || true
  echo "----- AM logs (recovery evidence) -----"
  yarn logs -applicationId "$APP" 2>/dev/null | grep -Ei "recover|relaunch|NodeManager.*lost|node.*lost|decommission|failed|restart" | head -60 || true
} > "$JH/exp07_jobhistory_$JOB.txt" 2>&1

curl -s "http://localhost:19888/ws/v1/history/mapreduce/jobs/$JOB/tasks" \
  > "$JH/exp07_tasks_$JOB.json" 2>/dev/null || true

say "--- task attempt states (expect extra FAILED attempts from NM loss) ---"
python3 - "$JOB" <<'EOF' 2>&1 | tee -a "$LOG"
import json, sys, urllib.request
job = sys.argv[1]
base = f'http://localhost:19888/ws/v1/history/mapreduce/jobs/{job}'
try:
    with urllib.request.urlopen(base + '/tasks') as r:
        tasks = json.load(r)['tasks']['task']
except Exception as e:
    print('history not ready:', e); sys.exit(0)
for t in tasks:
    if t['type'] != 'MAP':
        continue
    try:
        with urllib.request.urlopen(f"{base}/tasks/{t['id']}/attempts") as r:
            att = sorted(json.load(r)['taskAttempts'].get('taskAttempt') or [],
                         key=lambda x: x['id'])
    except Exception:
        continue
    print(t['id'], t['state'],
          ' '.join(a['id'].rsplit('_', 1)[-1] + '=' + a['state'] for a in att),
          f'(attempts={len(att)})')
EOF

say "--- output sample ---"
hdfs dfs -cat "$OUT/part-r-*" 2>/dev/null | head -10 | tee -a "$LOG"
say "RESULT=$([ "$ST" = 'FINISHED SUCCEEDED' ] && echo SUCCESS || echo FAILED) EXP=07 JOBID=$JOB"
echo "EXP07 done."
