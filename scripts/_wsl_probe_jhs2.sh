#!/bin/bash
J=job_1793346852065_0001
T=task_1793346852065_0001_m_000010
echo "=== HTML UI probes ==="
for u in \
  "http://localhost:19888/jobhistory/job/$J" \
  "http://localhost:19888/jobhistory/task/job/$J/$T" \
  "http://localhost:19888/jobhistory/jobs" ; do
  code=$(curl -s -o /tmp/p.html -w '%{http_code}' "$u")
  echo "== $code  $u  ($(wc -c < /tmp/p.html) bytes)"
done
echo
echo "=== JHS jar route strings ==="
cd /tmp
python3 - <<'EOF'
import zipfile, re
z = zipfile.ZipFile('/opt/hadoop-3.4.2/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-3.4.2.jar')
for n in z.namelist():
    if 'WebServices' in n and n.endswith('.class'):
        data = z.read(n)
        strs = re.findall(rb'[ -~]{5,}', data)
        print('---', n)
        for s in strs:
            t = s.decode()
            if '/' in t and ('job' in t or 'task' in t or 'attempt' in t.lower() or 'ws' in t):
                print('   ', t)
EOF
