import torch
from sklearn.metrics import mean_absolute_error
from models.dataset import LiveMatchDataLoader
from models.network.deep_emb_mlp import DeepEmbMLP
import pandas as pd
import numpy as np


class LiveMatchRegression(object):
    def __init__(self, all_df, label_idx_list, test_tournaments, if_mask_knock_off_matches,
                 batch_size=32, num_epochs=30, lr=5e-3, weight_decay=1e-3, max_token=100):
        self.label_config = {'frees_watching_match_rate': [1 / 0.24, 0.1],
                             "watch_time_per_free_per_match": [1 / 10.3, 1],
                             'subscribers_watching_match_rate': [1 / 0.56, 0.1],
                             "watch_time_per_subscriber_per_match": [1 / 57.9, 1],
                             "reach_rate": [1 / 57.9, 0.1]
        }
        self.batch_size = batch_size
        self.num_epochs = num_epochs
        self.max_token = max_token
        self.lr = lr
        self.weight_decay = weight_decay
        label_list_tmp = [label for label in self.label_config]
        self.label_list = [label_list_tmp[label_idx] for label_idx in label_idx_list]
        print(self.label_list)
        self.label_num = len(label_idx_list)
        self.model = DeepEmbMLP(columns=12, max_token=self.max_token, num_task=self.label_num)
        # print(self.model)
        self.loss_fn_list = [torch.nn.HuberLoss(delta=self.label_config[self.label_list[i]][1]) for i in range(self.label_num)]
        # self.loss_fn = torch.nn.HuberLoss(delta=0.1)
        #self.optimizer = torch.optim.SGD(self.model.parameters(), lr=1e-2)
        #self.optimizer = torch.optim.Adagrad(self.model.parameters(), lr=1e-2)
        # self.optimizer = torch.optim.Adam(self.model.parameters(), lr=5e-3, weight_decay=1e-3)
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=self.lr, weight_decay=self.weight_decay)
        # self.optimizer = torch.optim.Adam(self.model.parameters(), lr=1e-3)
        #self.optimizer = torch.optim.RMSprop(self.model.parameters(), lr=1e-2)
        self.dataset = LiveMatchDataLoader(dataset=all_df, label_list=self.label_list, test_tournaments=test_tournaments,
                                           if_mask_knock_off_matches=if_mask_knock_off_matches, max_token=self.max_token)
        self.if_mask_knock_off_matches_tag = "_masked" if if_mask_knock_off_matches else ""
        self.test_tournament = test_tournaments[0]

    def train(self):
        data_loader = self.dataset.get_dataset(batch_size=self.batch_size, mode='train')
        num_steps = len(data_loader)
        for epoch in range(self.num_epochs):
            for i, (x, y) in enumerate(data_loader):
                p = self.model(x)
                # loss = self.loss_fn(p, y.float())
                # print(y[0])
                # print(p[:, 0])
                if self.label_num == 1:
                    loss_list = [self.loss_fn_list[i](p, y[i].float()) for i in range(self.label_num)]
                else:
                    loss_list = [self.loss_fn_list[i](p[:, i], y[i].float()) for i in range(self.label_num)]
                loss = 0
                for idx in range(self.label_num):
                    # loss += self.label_config[data_loader.dataset.label_list[idx]][0] * loss_list[idx]
                    loss += loss_list[idx]
                # print(loss.dtype)
                loss /= len(loss_list)
                loss = loss_list[0]
                # print(f"loss = {loss}")
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
                # logging.info('Epoch [{}/{}], step [{}/{}], loss: {:.4f}'
                #              .format(epoch + 1, num_epochs, i + 1, num_steps, loss.item()))
                #for wp in self.model.parameters():
                #    print(wp.data.shape, wp.data)
            if epoch % 3 == 0:
                self.eval()

    def eval(self):
        data_loader = self.dataset.get_dataset(batch_size=64, mode='train')
        self.test_inner_overall(data_loader)

    def test(self):
        data_loader = self.dataset.get_dataset(batch_size=64, mode='test')
        self.test_inner_overall(data_loader, self.dataset.get_sample_ids('test'))

    def test_inner_overall(self, data_loader, sample_ids=None):
        live_ads_inventory_forecasting_root_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting"
        sample_ids_logs_all = []
        for label_idx in range(self.label_num):
            sample_ids_logs = self.test_inner(data_loader, label_idx, sample_ids)
            sample_ids_logs_all.append(sample_ids_logs)
        res_list = []
        cols = [f"content_id", f"estimated_{self.label_list[0]}", f"real_{self.label_list[0]}"]
        # print(cols)
        for sample_idx in range(len(sample_ids_logs_all[0])):
            items = [sample_ids_logs_all[0][sample_idx][0]]
            # items = []
            for label_idx in range(self.label_num):
                # items.append(f"{data_loader.dataset.label_list[label_idx]}: {sample_ids_logs_all[label_idx][sample_idx][1][0]}, {sample_ids_logs_all[label_idx][sample_idx][1][1]}")
                items.append(sample_ids_logs_all[label_idx][sample_idx][1][0])
                items.append(sample_ids_logs_all[label_idx][sample_idx][1][1])
                # print(sample_ids_logs_all[0][sample_idx][0].replace(" ", "_"), float(sample_ids_logs_all[label_idx][sample_idx][1][0]), float(sample_ids_logs_all[label_idx][sample_idx][1][1]))
            # print(items)
            res_list.append(items)
        df = pd.DataFrame(res_list, columns=cols)
        # if sample_ids is not None:
        #     for item in res_list:
        #         print(item)
        #         # if item[0].find('india vs australia') > -1:
        #         #     print(item)
        #             # 64
        #             # 100
        #             # 0.01
        #             # 0
        #             # 32
        #             # if 75.96 < item[1] < 75.99:
        #             #     print(item)
        #             #     print(self.batch_size)
        #             #     print(self.num_epochs)
        #             #     print(self.lr)
        #             #     print(self.weight_decay)
        #             #     print(self.max_token)
        #             # #     print()
        df.to_parquet(f"{live_ads_inventory_forecasting_root_path}/dnn_predictions{self.if_mask_knock_off_matches_tag}/{self.test_tournament}/{self.label_list[0]}")

    def test_inner(self, data_loader, idx, sample_ids=None):
        accloss = 0
        result = []
        labels = []
        test_num = 0
        for i, (x, y) in enumerate(data_loader):
            if self.label_num == 1:
                p = self.model(x).detach().numpy()
            else:
                p = self.model(x)[:, idx].detach().numpy()
            loss = mean_absolute_error(np.atleast_1d(p), y[idx])
            accloss += loss * len(y[idx])
            test_num += len(y[idx])
            if sample_ids is not None:
                result.extend([v for v in p])
                labels.extend([v.item() for v in y[idx]])
        print(f'Test mae error of {data_loader.dataset.label_list[idx]}: {accloss}, {accloss/test_num}')
        sample_ids_logs = []
        if sample_ids is not None:
            for i, item in enumerate(zip(result, labels)):
                # print(sample_ids[i], item)
                sample_ids_logs.append((sample_ids[i], item))
        return sample_ids_logs

    def save(self, path):
        torch.save(self.model.state_dict(), path)

    def load(self, path):
        self.model.load_state_dict(torch.load(path))
        self.model.eval()