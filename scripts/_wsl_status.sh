#!/bin/bash
echo "=== JAVA PROCESSES ==="
jps 2>/dev/null | sort
echo "=== YARN APPS (all) ==="
curl -s 'http://localhost:8088/ws/v1/cluster/apps' > /tmp/rmapps.json 2>/dev/null
python3 - <<'EOF'
import json
d = json.load(open('/tmp/rmapps.json'))
apps = d.get('apps', {}).get('app')
if not apps:
    print('(no applications)')
else:
    for a in apps:
        print(a['id'], a['state'], a['finalStatus'], str(a.get('progress')) + '%')
EOF
echo "=== RECENT DONE APPS incl finished ==="
yarn application -list -appStates ALL 2>/dev/null | tail -n +2
