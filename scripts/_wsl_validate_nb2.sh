#!/bin/bash
# Deep-check single.ipynb: compile every code cell for syntax errors,
# verify clean state (no outputs / null execution_count).
set -e
NB=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/single.ipynb
python3 - "$NB" <<'PY'
import json, sys
path = sys.argv[1]
nb = json.load(open(path, encoding='utf-8'))
errors = []
dirty = []
for i, c in enumerate(nb['cells']):
    if c['cell_type'] != 'code':
        continue
    src = ''.join(c['source'])
    first = src.strip().splitlines()[0] if src.strip() else '(empty)'
    try:
        compile(src, f'cell_{i}', 'exec')
    except SyntaxError as e:
        errors.append((i, first[:60], e.msg, e.lineno))
    if c.get('execution_count') is not None:
        dirty.append((i, 'execution_count', c.get('execution_count')))
    if c.get('outputs'):
        dirty.append((i, 'outputs', len(c['outputs'])))
if errors:
    print('SYNTAX ERRORS:')
    for e in errors:
        print('  cell', e)
else:
    print('all code cells compile: OK')
if dirty:
    print('NOT CLEAN (re-run state):')
    for d in dirty:
        print('  ', d)
else:
    print('clean state (no outputs, null counts): OK')
# count RUN flags and job ids referenced
src_all = '\n'.join(''.join(c['source']) for c in nb['cells'])
print('RUN flags:', src_all.count('RUN ='))
import re
print('job ids referenced:', sorted(set(re.findall(r'job_1793\d+_\d{4}', src_all))))
print('sections:', re.findall(r'^#+ (.+)$', src_all, re.M))
PY
