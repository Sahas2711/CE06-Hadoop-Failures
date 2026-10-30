#!/bin/bash
# =============================================================================
# CE-06: collect_evidence.sh - gathers ALL experiment evidence into
#   evidence/           (logs/, job_history/, cluster/, config/, summary)
# Safe to re-run; produces evidence/REPORT_EVIDENCE_SUMMARY.txt
# =============================================================================
set -u
PROJ=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.2
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

EV=$PROJ/evidence
JH=$EV/job_history
CL=$EV/cluster
CFG=$PROJ/config
SUM=$EV/REPORT_EVIDENCE_SUMMARY.txt
mkdir -p "$EV/logs" "$JH" "$CL" "$CFG"
: > "$SUM"

say() { echo "$@" | tee -a "$SUM"; }
JHS=http://localhost:19888
RMB=http://localhost:8088

say "==================================================================="
say "CE-06 EVIDENCE SUMMARY  -  collected $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
say "==================================================================="

# ---------- 1. cluster snapshot ----------
{
  echo "== jps =="; jps -l 2>/dev/null
  echo; echo "== yarn node -list -all =="; yarn node -list -all 2>&1
  echo; echo "== yarn application -list -appStates ALL =="; yarn application -list -appStates ALL 2>&1
  echo; echo "== hdfs dfs -ls -R /results /data =="; hdfs dfs -ls -R /results /data 2>&1
} > "$CL/cluster_snapshot.txt" 2>&1
say ""
say "[1] Cluster snapshot -> evidence/cluster/cluster_snapshot.txt"

# ---------- 2. config snapshots ----------
for f in core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml capacity-scheduler.xml hadoop-env.sh log4j.properties; do
  cp "$HADOOP_CONF_DIR/$f" "$CFG/$f" 2>/dev/null
done
{
  echo "== effective mapreduce settings (grep) =="
  grep -E "mapreduce\.(map|reduce)\.(maxattempts|speculative)|mapreduce.framework.name|mapreduce.job.speculative" \
    "$HADOOP_CONF_DIR/mapred-site.xml"
  echo; echo "== effective yarn settings (grep) =="
  grep -E "resource.memory|minimum-allocation|maximum-allocation|maximum-am-resource-percent|liveness-monitor|heartbeat" \
    "$HADOOP_CONF_DIR/yarn-site.xml" "$HADOOP_CONF_DIR/capacity-scheduler.xml"
} > "$CFG/effective_settings_grep.txt" 2>&1
cp /opt/hadoop-data/config-backups/* "$CFG/" 2>/dev/null
say "[2] Config snapshots -> config/"

# ---------- 3. per-job history evidence ----------
say ""
say "[3] JobHistory REST evidence -> evidence/job_history/"
JOBS=$(curl -s "$JHS/ws/v1/history/mapreduce/jobs?limit=100" \
  | python3 -c "import sys,json;[print(j['id']) for j in json.load(sys.stdin)['jobs']['job']]" 2>/dev/null)

python3 - "$JOBS" "$JHS" <<'EOF' | tee -a "$SUM"
import json, sys, urllib.request, collections
jobs = sys.argv[1].split()
jhs = sys.argv[2]
JH = '/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/job_history'
EXP = {
 '0001':'EXP1  NORMAL baseline', '0002':'EXP2a MALFORMED_RECORD_EXCEPTION maxattempts=2 (expect FAIL)',
 '0003':'EXP2b FAIL_ONCE retry-succeeds (expect SUCCESS)', '0004':'EXP3a ALWAYS_FAIL maxattempts=2 (expect FAIL)',
 '0005':'EXP3b ALWAYS_FAIL maxattempts=4 (expect FAIL)', '0006':'EXP4  SLOW_TASK spec OFF',
 '0007':'EXP5  SLOW_TASK spec ON (prelim sleep=150)', '0010':'EXP4v2 spec OFF sleep=400', '0011':'EXP5v2 spec ON sleep=400', '0008':'EXP6  KILL -9 task attempt',
 '0009':'EXP7  NodeManager failure'}
print()
print('%-6s %-46s %-10s %5s %6s %7s %7s %7s' %
      ('JOB','EXPERIMENT','STATE','tasks','attempts','succ','failed','killed'))
print('-'*104)
allrows = []
for jid in sorted(jobs):
    try:
        with urllib.request.urlopen(f'{jhs}/ws/v1/history/mapreduce/jobs/{jid}') as r:
            det = json.load(r)['job']
        with urllib.request.urlopen(f'{jhs}/ws/v1/history/mapreduce/jobs/{jid}/tasks') as r:
            tasks = json.load(r)['tasks']['task']
    except Exception as e:
        print(jid, 'ERR', e); continue
    n_succ = n_fail = n_kill = 0
    task_lines = []
    attempt_blobs = []
    for t in sorted(tasks, key=lambda x: x['id']):
        try:
            with urllib.request.urlopen(
                    f"{jhs}/ws/v1/history/mapreduce/jobs/{jid}/tasks/{t['id']}/attempts") as r:
                att = json.load(r)['taskAttempts'].get('taskAttempt') or []
        except Exception:
            att = []
        att = sorted(att, key=lambda x: x['id'])
        attempt_blobs.append({'task': t['id'], 'type': t['type'],
                              'taskState': t['state'],
                              'attempts': [{
                                  'id': a['id'], 'state': a['state'],
                                  'elapsed': a.get('elapsedTime'),
                                  'diagnostics': (a.get('diagnostics') or '')[:600]
                              } for a in att]})
        for a in att:
            if a['state'] == 'SUCCEEDED': n_succ += 1
            elif a['state'] == 'FAILED': n_fail += 1
            elif a['state'] == 'KILLED': n_kill += 1
        if t['type'] == 'MAP' or len(att) > 1 or t['state'] != 'SUCCEEDED':
            task_lines.append('  %-42s [%s/%s] %s' % (
                t['id'], t['type'], t['state'],
                ' '.join(a['id'].rsplit('_', 1)[-1] + '=' + a['state'] for a in att)))
    suffix = jid.split('_')[-1]
    expname = EXP.get(suffix, det['name'])
    n_att = n_succ + n_fail + n_kill
    print('%-6s %-46s %-10s %5d %6d %7d %7d %7d' %
          (suffix, expname, det['state'], len(tasks), n_att, n_succ, n_fail, n_kill))
    # save raw per-job evidence
    json.dump(det, open(f'{JH}/JOB_{jid}_detail.json', 'w'), indent=1)
    json.dump(attempt_blobs, open(f'{JH}/JOB_{jid}_task_attempts.json', 'w'), indent=1)
    # counters
    try:
        with urllib.request.urlopen(f'{jhs}/ws/v1/history/mapreduce/jobs/{jid}/counters') as r:
            json.dump(json.load(r), open(f'{JH}/JOB_{jid}_counters.json', 'w'), indent=1)
    except Exception:
        pass
    allrows.append((jid, expname, det['state'], task_lines))
print()
print('=' * 104)
print('PER-TASK ATTEMPT DETAIL (MAP tasks + any task with >1 attempt)')
print('=' * 104)
for jid, expname, state, lines in allrows:
    print(f'\n--- {jid}  {expname}  [{state}] ---')
    for l in lines:
        print(l)
EOF

# ---------- 4. RM application diagnostics ----------
say ""
say "[4] ResourceManager application records -> evidence/cluster/"
python3 - "$RMB" <<'EOF' | tee -a "$SUM"
import json, sys, urllib.request
rm = sys.argv[1]
CL = '/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/evidence/cluster'
with urllib.request.urlopen(f'{rm}/ws/v1/cluster/apps') as r:
    apps = (json.load(r).get('apps') or {}).get('app') or []
print('\n%-32s %-10s %-10s %-8s %s' % ('APPLICATION', 'STATE', 'FINAL', 'PROG%', 'DIAGNOSTICS (truncated)'))
print('-' * 120)
for a in sorted(apps, key=lambda x: x['id']):
    diag = (a.get('diagnostics') or '').replace('\n', ' ')[:180]
    print('%-32s %-10s %-10s %-8s %s' % (a['id'], a['state'], a['finalStatus'],
                                          str(a.get('progress', '')), diag))
    json.dump(a, open(f"{CL}/RM_{a['id']}.json", 'w'), indent=1)
EOF

# ---------- 5. failed-attempt stack traces from log aggregation ----------
say ""
say "[5] Aggregated YARN logs (CE06 exceptions for failed attempts) -> evidence/logs/"
for A in application_1793346852065_0002 application_1793346852065_0004 application_1793346852065_0005; do
  yarn logs -applicationId "$A" 2>/dev/null \
    | grep -B1 -A6 "CE06 simulated failure" > "$EV/logs/${A}_failure_stacktraces.log"
  N=$(grep -c "CE06 simulated failure" "$EV/logs/${A}_failure_stacktraces.log" 2>/dev/null || echo 0)
  say "    $A: $N CE06 exception occurrences captured"
done

# ---------- 6. output verification (identical results across successful runs) ----------
say ""
say "[6] Output verification (successful jobs must agree with baseline) -> evidence/"
{
  echo "Baseline exp01 vs others:"
  for D in exp02b_fail_once exp04_spec_off exp05_spec_on exp06_kill_attempt exp07_nodemanager_failure; do
    if hdfs dfs -test -d "/results/$D" 2>/dev/null; then
      if hdfs dfs -cat "/results/exp01_baseline/part-r-*" 2>/dev/null \
         | diff - <(hdfs dfs -cat "/results/$D/part-r-*" 2>/dev/null) > /tmp/diffout 2>&1; then
        echo "  IDENTICAL: /results/$D == /results/exp01_baseline"
      else
        echo "  DIFFERS:   /results/$D"; head -20 /tmp/diffout
      fi
    else
      echo "  (missing): /results/$D"
    fi
  done
  echo; echo "Baseline content:"
  hdfs dfs -cat /results/exp01_baseline/part-r-* 2>/dev/null
} | tee -a "$SUM" | tee "$EV/output_verification.txt"

# ---------- 7. job-history UI pages (HTML "screenshots") ----------
mkdir -p "$EV/ui"
for J in $JOBS; do
  curl -s "$JHS/jobhistory/job/$J" > "$EV/ui/jobpage_$J.html" 2>/dev/null
done
say ""
say "[7] JobHistory UI HTML pages -> evidence/ui/ ($(ls "$EV/ui" | wc -l) files)"

# ---------- 8. experiment console logs ----------
say ""
say "[8] Console logs -> evidence/logs/"
for L in "$EV"/logs/*_run.log; do
  [ -f "$L" ] || continue
  R=$(grep -h "^RESULT=" "$L" 2>/dev/null | tail -1)
  say "    $(basename "$L"): ${R:-no RESULT line}"
done

say ""
say "==================================================================="
say "Collection complete. Files:"
say "  evidence/REPORT_EVIDENCE_SUMMARY.txt   <- this summary"
say "  evidence/job_history/JOB_<id>_task_attempts.json  <- attempt states"
say "  evidence/job_history/JOB_<id>_counters.json       <- MR counters"
say "  evidence/cluster/RM_<appid>.json        <- RM app record + diagnostics"
say "  evidence/logs/*_run.log                 <- console logs"
say "  evidence/logs/*_failure_stacktraces.log <- exception stack traces"
say "  evidence/ui/                            <- JobHistory UI HTML"
say "  config/                                 <- cluster configs used"
say "==================================================================="
