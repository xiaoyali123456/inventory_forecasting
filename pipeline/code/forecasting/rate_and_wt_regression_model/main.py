import sys
import os
from datetime import datetime, timedelta

import pandas as pd

from dnn_regression import LiveMatchRegression
from dnn_configuration import *


def slack_notification(topic, region, message):
    cmd = f'aws sns publish --topic-arn "{topic}" --subject "midroll inventory forecasting" --message "{message}" --region {region}'
    # os.system(cmd)


def check_s3_path_exist(s3_path: str) -> bool:
    if not s3_path.endswith("/"):
        s3_path += "/"
    return os.system(f"aws s3 ls {s3_path}_SUCCESS") == 0


def get_date_list(date: str, days: int) -> list:
    dt = datetime.strptime(date, '%Y-%m-%d')
    if -1 <= days <= 1:
        return [date]
    elif days > 1:
        return [(dt + timedelta(days=n)).strftime('%Y-%m-%d') for n in range(0, days)]
    else:
        return [(dt + timedelta(days=n)).strftime('%Y-%m-%d') for n in range(days + 1, 1)]


def main(run_date):
    train_dataset = pd.read_parquet(f"{TRAIN_MATCH_TABLE_PATH}/cd={run_date}")
    filtered_df = train_dataset[train_dataset['tournament_name'].apply(lambda x: x[0] == 'icc cricket world cup')]
    filtered_df = filtered_df[filtered_df['if_contain_india_team'].apply(lambda x: x[0] == '0')]
    print(len(filtered_df))
    # train_dataset = train_dataset[train_dataset['content_id'] != '1540025169']
    # train_dataset = train_dataset[train_dataset['content_id'] != '1540025169']
    prediction_dataset = pd.read_parquet(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/")
    predict_filtered_df = prediction_dataset[prediction_dataset['tournament_name'].apply(lambda x: x[0] == 'icc cricket world cup')]
    predict_filtered_df = predict_filtered_df[predict_filtered_df['if_contain_india_team'].apply(lambda x: x[0] == '0')]
    print(len(filtered_df))
    if run_date >= "2023-10-16":
        train_dataset["tournament_name"] = train_dataset['tournament_name'].apply(lambda x:
                                          ["world cup" if "world cup" in a
                                           else a
                                           for a in x])
        prediction_dataset["tournament_name"] = prediction_dataset['tournament_name'].apply(lambda x:
                                                                                  ["world cup" if "world cup" in a
                                                                                   else a
                                                                                   for a in x])
    for key in DNN_CONFIGURATION['used_features']:
        train_dataset[key] = train_dataset[key].apply(lambda x: sorted(x))
        # prediction_dataset[key] = prediction_dataset[key].apply(lambda x: sorted(x))
    train_dataset_copy = train_dataset.copy()
    for key in DNN_CONFIGURATION['used_features']:
        train_dataset_copy[key] = train_dataset_copy[key].apply(lambda x: sorted(x, reverse=True))
    train_dataset = pd.concat([train_dataset, train_dataset_copy])
    # for key in [FREE_WT_LABEL, SUB_WT_LABEL]:
    #     train_dataset[key] = train_dataset[key].apply(lambda x: 15.0 if x < 15.0 else x)
    for label in LABEL_LIST[:4]:
        print(label)
        model = LiveMatchRegression(run_date, train_dataset, prediction_dataset, label)
        model.train()
        slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                           message=f"Train loss of {model.label} on {model.run_date}: {' -> '.join(model.train_loss_list)}")
        model.prediction(filtered_df, predict_filtered_df)


# main("2023-09-30")
# for run_date in get_date_list("2023-10-06", 19):
#     if check_s3_path_exist(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/"):
#         main(run_date)
#         slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
#                            message=f"rate and wt predictions on {run_date} are done.")
#     else:
#         slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
#                            message=f"rate and wt predictions on {run_date} nothing update.")


if __name__ == '__main__':
    run_date = sys.argv[1]
    if check_s3_path_exist(f"{PREDICTION_MATCH_TABLE_PATH}/cd={run_date}/"):
        main(run_date)
        slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                           message=f"rate and wt predictions on {run_date} are done.")
    else:
        slack_notification(topic=SLACK_NOTIFICATION_TOPIC, region=REGION,
                           message=f"rate and wt predictions on {run_date} nothing update.")
