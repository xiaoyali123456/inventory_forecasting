from glob import glob
import pandas as pd
import os

dates=[
'2021-09-21',
'2021-09-22',
'2021-09-23',
'2021-09-24',
'2021-09-25',
'2021-09-26',
'2021-09-27',
'2021-09-28',
'2021-09-29',
'2021-09-30',
'2021-10-01',
'2021-10-02',
'2021-10-03',
'2021-10-04',
'2021-10-05',
'2021-10-06',
'2021-10-07',
'2021-10-08',
'2021-10-10',
'2021-10-11',
'2021-10-13',
]

def curate(inp, out=None):
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
                        print('err1', inp, i)
                        break
                else:
                    break
        except:
            print('err2', inp, i)
    print(inp, cnt)
    if out:
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(inp, encoding='latin1') as fin:
            with open(out, 'w') as fout:
                for i, ln in enumerate(fin):
                    if i >= cnt:
                        fout.write(ln)

if __name__ == '__main__':
    for dt in dates:
        for f in glob(f'run_log/cd={dt}/*.csv'):
            curate(f, f.replace('run_log', 'run_log2'))
    header='Sr. No.,Start Date,Start Time,Playout ID,Content ID,Language,Tenant,Stream Type,Platform,Creative ID,Break ID,Creative Path,End Date,End Time,Actual Time,Delivered Time'.split(',')
    for dt in dates:
        for f in glob(f'run_log2/cd={dt}/*.csv'):
            try:
                df = pd.read_csv(f)
                if all(df.columns == header) == False:
                    print(f)
            except Exception as e:
                print(e)
                print(f)

# problem founded:
# 1. very long header count
# 2. 16 column vs 19/20/17 column: trailing comma at the end


'''
No columns to parse from file
run_log2/cd=2021-09-26/1540008482_P11_Singapore_Hindi_M38_CSKvsKKR.csv
No columns to parse from file
run_log2/cd=2021-09-26/1540008482_P18_Singapore_Tamil_M38_CSKvsKKR.csv
No columns to parse from file
run_log2/cd=2021-09-26/1540008482_P15_Kannada_India_M38_CSKvsKKR.csv
('Shapes must match', (19,), (16,))
run_log2/cd=2021-09-26/1540008482_P1_India_English_AOS_M38_CSKvsKKR.csv
No columns to parse from file
run_log2/cd=2021-09-26/1540008482_P21_India_Bengali_M38_CSKvsKKR.csv
('Shapes must match', (20,), (16,))
run_log2/cd=2021-09-26/1540008482_P17_Canada_Tamil_M38_CSKvsKKR.csv
No columns to parse from file
run_log2/cd=2021-09-26/1540008482_P22_India_Dugout_M38_CSKvsKKR.csv
No columns to parse from file
run_log2/cd=2021-09-26/1540008482_P23_India_Marathi_M38_CSKvsKKR.csv
('Shapes must match', (20,), (16,))
run_log2/cd=2021-09-26/1540008482_P20_India_Telugu_M38_CSKvsKKR.csv
Error tokenizing data. C error: Expected 1 fields in line 5, saw 5

run_log2/cd=2021-09-27/1540008488_P14_India_Dosts_CTV_M40_SRHvsRR.csv
No columns to parse from file
run_log2/cd=2021-09-28/1540008494_P2_India_English_IOS_M42_MIvsPBKS.csv
('Shapes must match', (17,), (16,))
run_log2/cd=2021-09-28/1540008494_P24_Dosts_AOS_M42_MIvsPBKS.csv
('Shapes must match', (17,), (16,))
run_log2/cd=2021-09-28/1540008491_P20_India_Telugu_M41_KKRvsDC.csv
No columns to parse from file
run_log2/cd=2021-09-28/1540008494_P15_India_Kannada_M42_MIvsPBKS.csv
('Shapes must match', (17,), (16,))
run_log2/cd=2021-09-28/1540008491_P3_India_English_CTV_M41_KKRvsDC.csv
('Shapes must match', (18,), (16,))
run_log2/cd=2021-09-28/1540008494_P16_India_Tamil_M42_MIvsPBKS.csv
('Shapes must match', (1,), (16,))
run_log2/cd=2021-10-02/1540008509_P24_Dosts_AOS_M47_RRvsCSK.csv
No columns to parse from file
run_log2/cd=2021-10-03/1540008512_P10_Canada_Hindi_M48_RCBvsPBKS.csv
('Shapes must match', (17,), (16,))
run_log2/cd=2021-10-03/1540008515_P2_India_English_IOS_M49_KKRvsSRH.csv
('Shapes must match', (20,), (16,))
run_log2/cd=2021-10-03/1540008512_P15_India_Kannada_M48_RCBvsPBKS.csv
('Shapes must match', (17,), (16,))
run_log2/cd=2021-10-03/1540008515_P1_India_English_AOS_M49_KKRvsSRH.csv
('Shapes must match', (17,), (16,))
run_log2/cd=2021-10-10/1540008539_P14_Dosts_CTV_M57_DCvsCSK.csv
'''