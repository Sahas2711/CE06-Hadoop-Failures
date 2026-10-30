#!/bin/bash
B="http://localhost:19888/ws/v1/history/mapreduce/jobs"
echo "=== one attempt object from job 0001 ==="
curl -s "$B/job_1793346852065_0001/tasks/task_1793346852065_0001_m_000010/attempts" \
  | python3 -m json.tool | head -40
echo "=== failed job 0002 tasks list ==="
curl -s "$B/job_1793346852065_0002/tasks" \
  | python3 -c "import sys,json;d=json.load(sys.stdin);ts=d['tasks']['task'];print(len(ts));[print(t['id'],t['type'],t['state']) for t in ts]"
echo "=== failed job 0002 attempt fetch m_000000 ==="
curl -s -o /tmp/a2.json -w '%{http_code}\n' "$B/job_1793346852065_0002/tasks/task_1793346852065_0002_m_000000/attempts"
head -c 600 /tmp/a2.json; echo
