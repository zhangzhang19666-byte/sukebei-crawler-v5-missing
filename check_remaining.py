#!/usr/bin/env python3
"""检查是否还有未消费的 missing ID"""
import json, glob, sys

prog = {}
try:
    prog = json.load(open('v5_progress.json'))
except:
    pass

for f in sorted(glob.glob('missing_*.txt')):
    dispatched = prog.get(f.split('/')[-1], 0)
    total = len([l for l in open(f) if l.strip().isdigit()])
    if dispatched < total:
        print('true')
        sys.exit(0)
print('false')
