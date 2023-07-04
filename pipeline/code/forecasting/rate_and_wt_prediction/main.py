import sys
import os

import pandas as pd

from dnn_regression import LiveMatchRegression
from dnn_configuration import *


def check_s3_path_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}_SUCCESS") == 0


def main(run_date):
    train_dataset = pd.read_parquet(f"{train_match_table_path}/cd={run_date}")
    prediction_dataset = pd.read_parquet(f"{prediction_match_table_path}/cd={run_date}/")
    for label in label_list:
        print(label)
        model = LiveMatchRegression(run_date, train_dataset, prediction_dataset, label)
        model.train()
        model.prediction()


if __name__ == '__main__':
    run_date = sys.argv[1]
    if check_s3_path_exist(f"{prediction_match_table_path}/cd={run_date}/"):
        main(run_date)

