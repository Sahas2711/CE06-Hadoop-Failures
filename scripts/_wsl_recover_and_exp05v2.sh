#!/bin/bash
# =============================================================================
# Recovery: start NodeManager detached (setsid), let stuck app 0010 (exp4v2)
# finish, collect its evidence, then run exp5v2 (spec ON pair partner) and
# collect its evidence.
# =============================================================================
set -u
PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
CLEAN=/data/clean/nasa_access_clean.log
EV=$PROJ/evidence
say() { echo "$@" | tee -a "$EV/logs/recovery_and_exp05v2.log"; }

say "=== RECOVERY + EXP5v2 $(date) ==="

# 1. start NM detached
if ! jps 2>/dev/null | grep -q NodeManager; then
  say "NodeManager down -> starting detached (setsid --fork yarn --daemon start)"
  setsid --fork yarn --daemon start nodemanager
fi
for i in $(seq 1 20); do
  ST=$(yarn node -list 2>/dev/null | awk '/RUNNING/ {print $1}' | head -1)
  [ -n "$ST" ] && break
  sleep 3
done
say "Node state: ${ST:-none} after $((i*3))s"
yarn node -list -all 2>/dev/null | tail -n +3 | tee -a "$EV/logs/recovery_and_exp05v2.log"

# 2. wait for app 0010 (exp4v2, submitted earlier and stuck in ACCEPTED)
APP10=application_1793346852065_0010
say "Waiting for $APP10 (exp4v2)..."
while true; do
  S=$(curl -s "http://localhost:8088/ws/v1/cluster/apps/$APP10" 2>/dev/null \
      | python3 -c "import sys,json;a=json.load(sys.stdin)['app'];print(a['state'],a['finalStatus'],a.get('progress'))" 2>/dev/null)
  say "  $S $(date +%T)"
  case "$S" in FINISHED*|FAILED*|KILLED*) break;; esac
  sleep 15
done

# 3. evidence for 0010
sleep 5
python3 - <<'EOF' | tee -a /mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/logs/recovery_and_exp05v2.log
import json, urllib.request
jhs = 'http://localhost:19888/ws/v1/history/mapreduce/jobs'
JH = '/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/job_history'
for jid in ['job_1793346852065_0010']:
    with urllib.request.urlopen(f'{jhs}/{jid}') as r:
        det = json.load(r)['job']
    with urllib.request.urlopen(f'{jhs}/{jid}/tasks') as r:
        tasks = json.load(r)['tasks']['task']
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
        if t['id'].endswith('_000000') or len(att) > 1:
            print(' ', t['id'], ' '.join(a['id'].rsplit('_',1)[-1] + '=' + a['state'] +
                  f"({a.get('elapsedTime')}ms)" for a in att))
    json.dump(blobs, open(f'{JH}/JOB_{jid}_task_attempts.json', 'w'), indent=1)
    json.dump(det, open(f'{JH}/JOB_{jid}_detail.json', 'w'), indent=1)
    try:
        with urllib.request.urlopen(f'{jhs}/{jid}/counters') as r:
            json.dump(json.load(r), open(f'{JH}/JOB_{jid}_counters.json', 'w'), indent=1)
    except Exception as e:
        print('counter err', e)
EOF

# 4. run exp5v2 with full evidence
say "=== EXP5 v2 (spec ON, sleep=400, caps=1.0) $(date) ==="
bash "$PROJ/scripts/run_experiment.sh" 05v2 /results/exp05_spec_on SLOW_TASK "$CLEAN" \
  -Dce06.sleep.ms=400 -Dmapreduce.map.speculative=true \
  -Dmapreduce.reduce.speculative=false \
  -Dmapreduce.job.speculative.speculativeCap=1.0 \
  -Dmapreduce.job.speculative.speculative-cap-total-tasks=1.0 \
  -Dmapreduce.job.speculative.speculative-cap-running-tasks=1.0

# 5. evidence for 0011
sleep 5
python3 - <<'EOF' | tee -a /mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/logs/recovery_and_exp05v2.log
import json, urllib.request
jhs = 'http://localhost:19888/ws/v1/history/mapreduce/jobs'
JH = '/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/job_history'
for jid in ['job_1793346852065_0011']:
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
            print(' ', t['id'], ' '.join(a['id'].rsplit('_',1)[-1] + '=' + a['state'] +
                  f"({a.get('elapsedTime')}ms)" for a in att))
    json.dump(blobs, open(f'{JH}/JOB_{jid}_task_attempts.json', 'w'), indent=1)
    json.dump(det, open(f'{JH}/JOB_{jid}_detail.json', 'w'), indent=1)
    try:
        with urllib.request.urlopen(f'{jhs}/{jid}/counters') as r:
            c = json.load(r)
        json.dump(c, open(f'{JH}/JOB_{jid}_counters.json', 'w'), indent=1)
        for g in c['jobCounters']['counterGroup']:
            if 'Job Counters' in g['counterGroupName']:
                print('  Job Counters:', {x['name']: x['totalCounterValue']
                      for x in g['counter']
                      if 'peculat' in x['name'] or 'Killed map' in x['name']
                      or 'Launched map' in x['name']})
    except Exception as e:
        print('counter err', e)
EOF
say "=== DONE $(date) ==="
