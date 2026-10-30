#!/bin/bash
J=job_1793346852065_0007
curl -s "http://localhost:19888/ws/v1/history/mapreduce/jobs/$J/conf" > /tmp/j7conf.json
python3 - <<'EOF'
import json
d = json.load(open('/tmp/j7conf.json'))
props = d['conf']['property']
want = ['mapreduce.map.speculative', 'mapreduce.reduce.speculative',
        'mapreduce.job.speculative.speculativeCap',
        'mapreduce.job.speculative.slowstartcompletedmaps',
        'mapreduce.map.maxattempts', 'mapreduce.framework.name',
        'yarn.app.mapreduce.am.resource.mb']
for p in props:
    if p['name'] in want:
        print(p['name'], '=', p['value'])
print('---- all speculative* props ----')
for p in props:
    if 'speculative' in p['name']:
        print(p['name'], '=', p['value'])
EOF
echo "=== exp7 exact attempt IDs (post-recovery) ==="
python3 - <<'EOF'
import json
d = json.load(open('/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/job_history/JOB_job_1793346852065_0009_task_attempts.json'))
for t in d[:4]:
    print(t['task'], t['taskState'])
    for a in t['attempts']:
        print('   ', a['id'], a['state'], 'elapsed=', a['elapsed'])
EOF
