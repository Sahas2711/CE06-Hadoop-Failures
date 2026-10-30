#!/bin/bash
# FINAL UNIFIED: complete remaining experiments + all evidence. One shot.
set -u
PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
CLEAN=/data/clean/nasa_access_clean.log
EV=$PROJ/evidence
LOG=$EV/logs/final_run.log
say() { echo "$@" | tee -a "$LOG"; }
say "=== FINAL RUN $(date) ==="

# stop any stale orchestrator from the earlier attempt (avoid double-submit)
pkill -f _wsl_recover_and_exp05v2 2>/dev/null && say "killed stale orchestrator" || true

# NM must be up (detached)
if ! jps 2>/dev/null | grep -q NodeManager; then
  setsid --fork yarn --daemon start nodemanager; say "started NM"
fi
for i in $(seq 1 15); do
  yarn node -list 2>/dev/null | grep -q RUNNING && break; sleep 2; done
say "NM: $(yarn node -list 2>/dev/null | awk '/RUNNING/{print $1; exit}')"

wait_app() { # $1=appid
  while true; do
    S=$(curl -s "http://localhost:8088/ws/v1/cluster/apps/$1" 2>/dev/null \
        | python3 -c "import sys,json;a=json.load(sys.stdin)['app'];print(a['state'],a['finalStatus'])" 2>/dev/null)
    case "$S" in FINISHED*|FAILED*|KILLED*) say "  $1 final: $S"; break;; esac
    say "  $1: $S"; sleep 12
  done
}

# 1. finish exp4v2 (0010)
say "[1] exp4v2 job 0010"
wait_app application_1793346852065_0010

# 2. exp5v2: submit if not already present, else just wait
if curl -s "http://localhost:8088/ws/v1/cluster/apps/application_1793346852065_0011" \
     | grep -q '"id"'; then
  say "[2] exp5v2 job 0011 already exists -> waiting (its own client may be gone)"
  wait_app application_1793346852065_0011
  CLIENT=orphan
else
  say "[2] running exp5v2 fresh (spec ON sleep=400 caps=1.0)"
  bash "$PROJ/scripts/run_experiment.sh" 05v2 /results/exp05_spec_on SLOW_TASK "$CLEAN" \
    -Dce06.sleep.ms=400 -Dmapreduce.map.speculative=true \
    -Dmapreduce.reduce.speculative=false \
    -Dmapreduce.job.speculative.speculativeCap=1.0 \
    -Dmapreduce.job.speculative.speculative-cap-total-tasks=1.0 \
    -Dmapreduce.job.speculative.speculative-cap-running-tasks=1.0
  CLIENT=full
fi

# 3. fetch attempt evidence for 0010 + 0011
say "[3] fetching attempt evidence 0010/0011"
sleep 4
python3 - <<'EOF' | tee -a /mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/logs/final_run.log
import json, urllib.request
jhs = 'http://localhost:19888/ws/v1/history/mapreduce/jobs'
JH = '/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/job_history'
for jid in ['job_1793346852065_0010', 'job_1793346852065_0011']:
    try:
        with urllib.request.urlopen(f'{jhs}/{jid}') as r:
            det = json.load(r)['job']
        with urllib.request.urlopen(f'{jhs}/{jid}/tasks') as r:
            tasks = json.load(r)['tasks']['task']
    except Exception as e:
        print(jid, 'ERR', e); continue
    print(f"--- {jid} {det['name']} [{det['state']}] ---")
    blobs = []
    for t in sorted(tasks, key=lambda x: x['id']):
        with urllib.request.urlopen(f"{jhs}/{jid}/tasks/{t['id']}/attempts") as r:
            att = sorted(json.load(r)['taskAttempts'].get('taskAttempt') or [],
                         key=lambda x: x['id'])
        blobs.append({'task': t['id'], 'type': t['type'], 'taskState': t['state'],
                      'attempts': [{'id': a['id'], 'state': a['state'],
                                    'elapsed': a.get('elapsedTime'),
                                    'diagnostics': (a.get('diagnostics') or '')[:400]}
                                   for a in att]})
        if t['type'] == 'MAP' and (len(att) > 1 or t['id'].endswith('_000000')):
            print(' ', t['id'], t['state'],
                  ' '.join(a['id'].rsplit('_',1)[-1] + '=' + a['state'] +
                           f"({a.get('elapsedTime')}ms)" for a in att))
    json.dump(blobs, open(f'{JH}/JOB_{jid}_task_attempts.json', 'w'), indent=1)
    json.dump(det, open(f'{JH}/JOB_{jid}_detail.json', 'w'), indent=1)
    try:
        with urllib.request.urlopen(f'{jhs}/{jid}/counters') as r:
            c = json.load(r)
        json.dump(c, open(f'{JH}/JOB_{jid}_counters.json', 'w'), indent=1)
        for g in c['jobCounters']['counterGroup']:
            if 'Job Counters' in g['counterGroupName']:
                print('  ', jid[-4:], {x['name']: x['totalCounterValue']
                      for x in g['counter']
                      if 'peculat' in x['name'] or 'Killed map' in x['name']
                      or 'Launched map' in x['name']})
    except Exception as e:
        print('counter err', e)
EOF

# 4. full evidence collection pass (now covers ALL jobs incl 0010/0011)
say "[4] collect_evidence.sh"
bash "$PROJ/scripts/collect_evidence.sh" > "$EV/collect_last_pass.txt" 2>&1
tail -5 "$EV/collect_last_pass.txt"

say "=== FINAL RUN COMPLETE $(date) ==="
grep -h "^RESULT=" "$EV"/logs/*.log 2>/dev/null | tee -a "$LOG"
