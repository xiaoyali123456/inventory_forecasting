import sys
import os

import pandas as pd

from dnn_regression import LiveMatchRegression
from dnn_configuration import *


def slack_notification(topic, region, message):
    cmd = f'aws sns publish --topic-arn "{topic}" --subject "midroll inventory forecasting" --message "{message}" --region {region}'
    os.system(cmd)


def check_s3_path_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}_SUCCESS") == 0


def main(run_date):
    train_dataset = pd.read_parquet(f"{TRAIN_MATCH_TABLE_PATH}/cd={run_date}")
    # train_dataset = train_dataset[train_dataset['content_id'] != '1540025169']
    # train_dataset = train_dataset[train_dataset['content_id'] != '1540025169']
    prediction_dataset = pd.read_parquet(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/")
    train_dataset["tournament_name"] = train_dataset['tournament_name'].apply(lambda x:
                                      ["world cup" if "world cup" in a
                                       else a
                                       for a in x])
    prediction_dataset["tournament_name"] = prediction_dataset['tournament_name'].apply(lambda x:
                                                                              ["world cup" if "world cup" in a
                                                                               else a
                                                                               for a in x])
    for label in LABEL_LIST:
        print(label)
        model = LiveMatchRegression(run_date, train_dataset, prediction_dataset, label)
        model.train()
        slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                           message=f"Train loss of {model.label} on {model.run_date}: {' -> '.join(model.train_loss_list)}")
        model.prediction()


if __name__ == '__main__':
    run_date = sys.argv[1]
    if check_s3_path_exist(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/"):
        main(run_date)
        slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                           message=f"rate and wt predictions on {run_date} are done.")
    else:
        slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                           message=f"rate and wt predictions on {run_date} nothing update.")

