#!/bin/bash
# Fetch task-attempt evidence for ALL experiments from JobHistoryServer.
set -u
PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
JH=$PROJ/evidence/job_history
mkdir -p "$JH"
JOBS=$(curl -s 'http://localhost:19888/ws/v1/history/mapreduce/jobs?limit=50' \
  | python3 -c "import sys,json;[print(j['id']) for j in json.load(sys.stdin)['jobs']['job']]")
echo "History jobs found:"; echo "$JOBS"
echo
for JOB in $JOBS; do
  echo "############ $JOB ############"
  curl -s "http://localhost:19888/ws/v1/history/mapreduce/jobs/$JOB" > "$JH/job_${JOB}_detail.json"
  curl -s "http://localhost:19888/ws/v1/history/mapreduce/jobs/$JOB/tasks" > "$JH/job_${JOB}_tasks.json"
  python3 - "$JOB" <<'EOF'
import json, sys, urllib.request, collections
job = sys.argv[1]
base = f'http://localhost:19988'  # placeholder
base = f'http://localhost:19888/ws/v1/history/mapreduce/jobs/{job}'
det = json.load(open(f'/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/job_history/job_{job}_detail.json'))
j = det['job']
print(f"name={j['name']}  state={j['state']}  finished={j.get('finishedTime')}")
tasks = json.load(open(f'/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/job_history/job_{job}_tasks.json'))['tasks']['task']
per_task = {}
type_counts = collections.Counter()
for t in tasks:
    type_counts[(t['type'], t['state'])] += 1
    with urllib.request.urlopen(f"{base}/tasks/{t['id']}/attempts") as r:
        att = sorted(json.load(r)['taskAttempts']['taskAttempt'], key=lambda x: x['id'])
    per_task[t['id']] = (t['type'], t['state'], att)
n_att = sum(len(a) for _, _, a in per_task.values())
n_failed = sum(1 for _, _, a in per_task.values() for x in a if x['status'] == 'FAILED')
n_killed = sum(1 for _, _, a in per_task.values() for x in a if x['status'] == 'KILLED')
n_succ = sum(1 for _, _, a in per_task.values() for x in a if x['status'] == 'SUCCEEDED')
print(f"tasks={len(tasks)}  attempts={n_att} (succeeded={n_succ} failed={n_failed} killed={n_killed})")
print("task states:", dict(type_counts))
# print per-task attempt lines; for MAP always; for others only if >1 attempt
for tid in sorted(per_task):
    typ, tstate, att = per_task[tid]
    if typ == 'MAP' or len(att) > 1 or tstate != 'SUCCEEDED':
        line = '  ' + tid + f' [{typ} {tstate}] ' + ' '.join(
            a['id'].rsplit('_', 1)[-1] + '=' + a['status'] + (
                f"({a.get('rackFailureReason','')[:40]})" if False else '') for a in att)
        print(line)
EOF
  echo
done
echo "Evidence JSONs saved in $JH"
