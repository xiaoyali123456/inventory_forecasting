import os
import torch
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error
from datetime import datetime, timedelta

from data_loader import LiveMatchDataLoader
from network import DeepEmbMLP
from dnn_configuration import *


def get_date_list(date: str, days: int) -> list:
    dt = datetime.strptime(date, '%Y-%m-%d')
    if -1 <= days <= 1:
        return [date]
    elif days > 1:
        return [(dt + timedelta(days=n)).strftime('%Y-%m-%d') for n in range(0, days)]
    else:
        return [(dt + timedelta(days=n)).strftime('%Y-%m-%d') for n in range(days + 1, 1)]


# dnn regression model for live matches prediction
class LiveMatchRegression(object):
    def __init__(self, run_date, train_dataset, prediction_dataset, label):
        self.run_date = run_date
        self.label = label
        self.model = DeepEmbMLP(column_num=len(DNN_CONFIGURATION['used_features']))
        self.loss_fn = torch.nn.HuberLoss(delta=HUBER_LOSS_PARAMETER_DIC[label])
        self.optimizer = torch.optim.Adam(self.model.parameters(),
                                          lr=DNN_CONFIGURATION['lr'], weight_decay=DNN_CONFIGURATION['weight_decay'])
        self.dataset = LiveMatchDataLoader(train_dataset=train_dataset, prediction_dataset=prediction_dataset,
                                           label=label)
        self.train_loss_list = []
        self.model_version = ""
        self.epoch = DNN_CONFIGURATION['epoch_num']

    def train(self):
        data_loader = self.dataset.get_dataset(batch_size=DNN_CONFIGURATION['train_batch_size'], mode='train')
        for epoch in range(self.epoch):
            for i, (x, y) in enumerate(data_loader):
                p = self.model(x)
                loss = self.loss_fn(p, y.float())
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
            # if epoch % 3 == 0:
            #     print(epoch)
            #     self.eval()
        torch.save(self.model.state_dict(), f'model_{self.label}.pth')
        os.system(f"aws s3 cp model_{self.label}.pth {PIPELINE_BASE_PATH}/dnn_models/cd={self.run_date}/")

    def eval(self):
        data_loader = self.dataset.get_dataset(batch_size=DNN_CONFIGURATION['test_batch_size'], mode='train')
        self.eval_inner(data_loader)

    def prediction(self, filtered_df, predict_filtered_df):
        data_loader = self.dataset.get_dataset(batch_size=DNN_CONFIGURATION['test_batch_size'], mode='prediction')
        self.prediction_inner(data_loader, self.dataset.get_sample_ids('prediction'), filtered_df, predict_filtered_df)

    # calculate loss
    def eval_inner(self, data_loader):
        mae_loss = 0.0
        test_num = 0
        for i, (x, y) in enumerate(data_loader):
            p = self.model(x).detach().numpy()
            loss = mean_absolute_error(np.atleast_1d(p), y)  # use np.atleast_1d in case there is only one sample
            mae_loss += loss * len(y)
            test_num += len(y)
        print(f'Test mae error of {self.label}: {mae_loss}, {mae_loss / test_num}')
        self.train_loss_list.append(str(mae_loss))

    def prediction_inner(self, data_loader, sample_ids, filtered_df, predict_filtered_df):
        # make prediction
        predictions = []
        for i, (x, y) in enumerate(data_loader):
            p = self.model(x).detach().numpy()
            predictions.extend([v for v in np.atleast_1d(p)])  # use np.atleast_1d in case there is only one sample
        prediction_results = []
        for idx in range(len(sample_ids)):
            prediction_results.append([sample_ids[idx], predictions[idx]])

        # adjust outliers if the predicted value is smaller than mean - std of non-india matches predictions
        cols = ["content_id", f"estimated_{self.label}"]
        df = pd.DataFrame(prediction_results, columns=cols)
        print(df)
        if len(predict_filtered_df) > 0:
            print(df[f"estimated_{self.label}"].mean(), df[f"estimated_{self.label}"].std())
            if len(filtered_df) > 0:
                entire_tournament_df = pd.concat(
                    [filtered_df.rename(columns={self.label: f"estimated_{self.label}"})[["content_id", f"estimated_{self.label}"]],
                     pd.merge(df, predict_filtered_df[["content_id"]], on='content_id', how='inner')[["content_id", f"estimated_{self.label}"]]])
            else:
                entire_tournament_df = pd.merge(df, predict_filtered_df[["content_id", self.label]], on='content_id', how='inner')[["content_id", f"estimated_{self.label}"]]
            # print(entire_tournament_df)
            t_mean = entire_tournament_df[f"estimated_{self.label}"].mean()
            t_std = entire_tournament_df[f"estimated_{self.label}"].std()
            lower_bound = t_mean - t_std
            print(t_mean, t_std, lower_bound)
            df[f"estimated_{self.label}"] = df[f"estimated_{self.label}"].apply(lambda x: lower_bound if x < lower_bound else x)
        print(df)
        df.to_parquet(f"{PIPELINE_BASE_PATH}/dnn_predictions{self.model_version}/cd={self.run_date}/label={self.label}")

    def prediction_on_training_dataset(self, filtered_df):
        data_loader = self.dataset.get_dataset(batch_size=DNN_CONFIGURATION['test_batch_size'], mode='train_for_prediction')
        self.prediction_on_training_dataset_inner(data_loader, self.dataset.get_sample_ids('train_for_prediction'), filtered_df)

    def prediction_on_training_dataset_inner(self, data_loader, sample_ids, filtered_df):
        predictions = []
        for i, (x, y) in enumerate(data_loader):
            p = self.model(x).detach().numpy()
            predictions.extend([v for v in np.atleast_1d(p)])  # use np.atleast_1d in case there is only one sample
        prediction_results = []
        for idx in range(len(sample_ids)):
            prediction_results.append([sample_ids[idx], predictions[idx]])

        cols = ["content_id", f"estimated_{self.label}"]
        df = pd.DataFrame(prediction_results, columns=cols) \
            .groupby('content_id').agg({f"estimated_{self.label}": 'mean'})
        print(df)
        if len(filtered_df) > 0:
            entire_tournament_df = pd.merge(df, filtered_df[["content_id"]], on='content_id', how='inner')[
                ["content_id", f"estimated_{self.label}"]]
            print(len(entire_tournament_df))
            t_mean = entire_tournament_df[f"estimated_{self.label}"].mean()
            t_std = entire_tournament_df[f"estimated_{self.label}"].std()
            lower_bound = t_mean - t_std
            print(t_mean, t_std, lower_bound)
            df[f"estimated_{self.label}"] = df[f"estimated_{self.label}"].apply(lambda x: lower_bound if x < lower_bound else x)
        df.to_parquet(f"{PIPELINE_BASE_PATH}/dnn_train_predictions{self.model_version}/cd={self.run_date}/label={self.label}")

    def save(self, path):
        torch.save(self.model.state_dict(), path)

    def load(self, path):
        self.model.load_state_dict(torch.load(path))
        self.model.eval()
