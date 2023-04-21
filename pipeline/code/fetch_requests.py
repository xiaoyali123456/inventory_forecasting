import json
import sys

import pandas as pd
import s3fs
from common import REQUESTS_PATH_TEMPL, SERVER_URL_ROOT

def main(cd):
    req_lst = []
    i, tot = 1, 1
    size = 10
    while i <= tot:
        df = pd.read_json(f'{SERVER_URL_ROOT}inventory/forecast-request?'
                        'status=INIT'
                        f'&page-number={i}'
                        f'&page-size={size}'
                        )
        req_lst += df.results.tolist()
        tot = df.total_pages[0]
        i += 1
    s3 = s3fs.S3FileSystem()
    with s3.open(REQUESTS_PATH_TEMPL % cd, 'w') as f:
        json.dump(req_lst, f)


if __name__ == '__main__':
    cd = sys.argv[1]
    main(cd)
