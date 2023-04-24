import pandas as pd
import sys
from functools import reduce
from common import *

if __name__ == '__main__':
    DATE = sys.argv[1]
    ad_time_ratio = pd.read_paquert(f'{AD_TIME_SAMPLING_PATH}cd={DATE}/')
    reach_ratio = pd.read_paquert(f'{REACH_SAMPLING_PATH}cd={DATE}/')

