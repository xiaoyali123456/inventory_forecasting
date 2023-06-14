from dnn_regression import LiveMatchRegression
from dnn_configuration import *
import pandas as pd
import sys


def main(DATE):
    train_dataset = pd.read_parquet(f"{pipeline_base_path}/all_features_hots_format_full_avod_and_simple_one_hot/")
    test_dataset = pd.read_parquet(f"{pipeline_base_path}/prediction/all_features_hots_format_and_simple_one_hot/cd={DATE}/")
    for label in label_list:
        model = LiveMatchRegression(DATE, train_dataset, test_dataset, label)
        model.train()
        model.test()


if __name__ == '__main__':
    DATE = sys.argv[1]
    main(DATE)

