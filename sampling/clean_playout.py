header='Sr. No.,Start Date,Start Time,Playout ID,Content ID,Language,Tenant,Stream Type,Platform,Creative ID,Break ID,Creative Path,End Date,End Time,Actual Time,Delivered Time'
import sys
import os
inp = sys.argv[1]
out = None
if len(sys.argv) == 3:
    out = sys.argv[2]
beforeHead = True
cnt = 0
with open(inp, encoding='latin1') as fin:
    try:
        for i, ln in enumerate(fin):
            if beforeHead:
                if ',,,,,' in ln:
                    cnt+=1
                    continue
                elif 'Playout ID' in ln:
                    beforeHead = False
                else:
                    print('err1', i)
                    break
            else:
                break
    except:
        print('err2', i)
print(inp, cnt)
if out:
    os.system(f'mkdir -p $(dirname "{out}")')
    with open(inp, encoding='latin1') as fin:
        with open(out, 'w') as fout:
            for i, ln in enumerate(fin):
                if i >= cnt:
                    fout.write(ln)
