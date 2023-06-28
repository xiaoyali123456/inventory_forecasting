from dnn_regression import LiveMatchRegression
from dnn_configuration import *
import pandas as pd
import sys, os


def check_s3_path_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}_SUCCESS") == 0


def main(DATE):
    train_dataset = pd.read_parquet(training_data_path)
    prediction_dataset = pd.read_parquet(f"{prediction_feature_path}/cd={DATE}/")
    for label in label_list:
        print(label)
        model = LiveMatchRegression(DATE, train_dataset, prediction_dataset, label)
        model.train()
        model.prediction()


if __name__ == '__main__':
    DATE = sys.argv[1]
    if check_s3_path_exist(f"{prediction_feature_path}/cd={DATE}/"):
        main(DATE)

