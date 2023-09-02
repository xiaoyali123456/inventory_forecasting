# parse ssai
import pandas as pd

dc = {}
for r in y.itertuples():
    for s in r.a.split(':'):
        if '_' in s:
            head, tail = s.split('_', 1)
            lst = dc.setdefault(head, [])
            lst.append(tail)
    if 'P_' not in r.a:
        dc['P'].append('<nan>')

for k in dc:
    print(k, len(dc[k]))

cfg = pd.DataFrame(dc)
