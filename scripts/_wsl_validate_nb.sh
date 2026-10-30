#!/bin/bash
# Validate single.ipynb JSON / nbformat structure
set -e
NB=/mnt/d/sem7/BDA-assignment/CE06_Hadoop_Practical/single.ipynb
python3 - "$NB" <<'PY'
import json, sys, collections
path = sys.argv[1]
nb = json.load(open(path, encoding='utf-8'))
print('valid JSON')
print('nbformat:', nb.get('nbformat'), nb.get('nbformat_minor'))
cells = nb['cells']
print('cells:', len(cells))
print('kernelspec:', nb.get('metadata', {}).get('kernelspec'))
print('cell types:', dict(collections.Counter(c['cell_type'] for c in cells)))
# structural checks
bad = []
for i, c in enumerate(cells):
    if c['cell_type'] not in ('code', 'markdown'):
        bad.append((i, 'unknown cell_type'))
    if 'source' not in c or not isinstance(c['source'], list):
        bad.append((i, 'source not a list'))
    if c['cell_type'] == 'code':
        if 'outputs' not in c or not isinstance(c['outputs'], list):
            bad.append((i, 'outputs not a list'))
        if 'execution_count' not in c:
            bad.append((i, 'no execution_count'))
# try nbformat validation if available
try:
    import nbformat
    nbformat.validate(nbformat.reads(open(path, encoding='utf-8').read(), as_version=4))
    print('nbformat.validate: OK')
except ImportError:
    print('nbformat.validate: skipped (module not installed)')
except Exception as e:
    bad.append(('nbformat', repr(e)))
if bad:
    print('PROBLEMS:')
    for b in bad:
        print('  ', b)
    sys.exit(1)
print('structure: OK')
PY
