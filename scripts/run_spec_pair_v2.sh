#!/bin/bash
# =============================================================================
# EXP 4 v2 + EXP 5 v2: controlled speculative-execution pair.
# Why v2: with sleep=150ms the straggler finished while all map slots were
# still busy with fast tasks -> no slot ever freed for a speculative duplicate.
# sleep=400ms makes the straggler (partition 0, attempt _0) outlive every fast
# map, so free slots exist at the moment speculative eligibility triggers.
# All speculative caps set to 1.0 so small (13-task) jobs are not blocked by
# the default speculative-cap-total-tasks=0.01.
# =============================================================================
set -u
PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
CLEAN=/data/clean/nasa_access_clean.log

echo "##### EXP4 v2 (spec OFF, sleep=400) start $(date) #####"
bash "$PROJ/scripts/run_experiment.sh" 04v2 /results/exp04_spec_off SLOW_TASK "$CLEAN" \
  -Dce06.sleep.ms=400 -Dmapreduce.map.speculative=false -Dmapreduce.reduce.speculative=false
echo; echo ">>> EXP4v2 done (expect SUCCESS, 1 attempt everywhere, straggler waited)"; echo

echo "##### EXP5 v2 (spec ON, sleep=400, caps=1.0) start $(date) #####"
bash "$PROJ/scripts/run_experiment.sh" 05v2 /results/exp05_spec_on SLOW_TASK "$CLEAN" \
  -Dce06.sleep.ms=400 -Dmapreduce.map.speculative=true \
  -Dmapreduce.reduce.speculative=false \
  -Dmapreduce.job.speculative.speculativeCap=1.0 \
  -Dmapreduce.job.speculative.speculative-cap-total-tasks=1.0 \
  -Dmapreduce.job.speculative.speculative-cap-running-tasks=1.0
echo; echo ">>> EXP5v2 done (expect SUCCESS, task 000000: _0 KILLED + _1 SUCCEEDED)"; echo

echo "##### fetching attempt-level evidence for jobs 0010/0011 #####"
sleep 5
python3 - <<'EOF'
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
        print(jid, 'not in history yet:', e); continue
    print(f"\n--- {jid}  {det['name']}  [{det['state']}] ---")
    blobs = []
    for t in sorted(tasks, key=lambda x: x['id']):
        try:
            with urllib.request.urlopen(f"{jhs}/{jid}/tasks/{t['id']}/attempts") as r:
                att = sorted(json.load(r)['taskAttempts'].get('taskAttempt') or [],
                             key=lambda x: x['id'])
        except Exception:
            att = []
        blobs.append({'task': t['id'], 'type': t['type'], 'taskState': t['state'],
                      'attempts': [{'id': a['id'], 'state': a['state'],
                                    'elapsed': a.get('elapsedTime'),
                                    'diagnostics': (a.get('diagnostics') or '')[:400]}
                                   for a in att]})
        if t['type'] == 'MAP' and (len(att) > 1 or t['id'].endswith('_000000')):
            print(' ', t['id'], ' '.join(a['id'].rsplit('_', 1)[-1] + '=' + a['state'] +
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
                      if 'peculat' in x['name'] or 'Killed' in x['name']
                      or 'Launched' in x['name'] or 'Failed' in x['name']})
    except Exception as e:
        print('  counters err', e)
EOF
echo "##### v2 pair complete #####"
