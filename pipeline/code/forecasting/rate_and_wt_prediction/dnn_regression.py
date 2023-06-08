import torch
from sklearn.metrics import mean_absolute_error
from rate_and_wt_prediction.data_loader import LiveMatchDataLoader
from rate_and_wt_prediction.network import DeepEmbMLP
from path import *
from util import *
from config import *


class LiveMatchRegression(object):
    def __init__(self, DATE, train_df, test_df, label):
        self.DATE = DATE
        self.label = label
        self.model = DeepEmbMLP(columns=len(dnn_configuration['used_features']),
                                max_token=dnn_configuration['embedding_table_size'])
        self.loss_fn = torch.nn.HuberLoss(delta=huber_loss_parameter_dic[label])
        self.optimizer = torch.optim.Adam(self.model.parameters(),
                                          lr=dnn_configuration['lr'], weight_decay=dnn_configuration['weight_decay'])
        self.dataset = LiveMatchDataLoader(train_dataset=train_df, test_dataset=test_df, label=label)

    def train(self):
        data_loader = self.dataset.get_dataset(batch_size=dnn_configuration['train_batch_size'], mode='train')
        for epoch in range(dnn_configuration['epoch_num']):
            for i, (x, y) in enumerate(data_loader):
                p = self.model(x)
                loss = self.loss_fn(p, y.float())
                self.optimizer.zero_grad()
                loss.backward()
                self.optimizer.step()
            if epoch % 3 == 0:
                self.eval()

    def eval(self):
        data_loader = self.dataset.get_dataset(batch_size=dnn_configuration['test_batch_size'], mode='train')
        self.test_inner(data_loader)

    def test(self):
        data_loader = self.dataset.get_dataset(batch_size=dnn_configuration['test_batch_size'], mode='test')
        self.test_inner(data_loader, self.dataset.get_sample_ids('test'))

    def test_inner(self, data_loader, sample_ids=None):
        mae_loss = 0.0
        predictions = []
        labels = []
        test_num = 0
        for i, (x, y) in enumerate(data_loader):
            p = self.model(x).detach().numpy()
            loss = mean_absolute_error(p, y)
            mae_loss += loss * len(y)
            test_num += len(y)
            if sample_ids is not None:
                predictions.extend([v for v in p])
                labels.extend([v.item() for v in y])
        print(f'Test mae error of {self.label}: {mae_loss}, {mae_loss/test_num}')
        if sample_ids is not None:
            sample_ids_logs = []
            for idx in range(len(sample_ids)):
                sample_ids_logs.append([sample_ids[idx], predictions[idx], labels[idx]])
            cols = [f"content_id", f"estimated_{self.label}", f"real_{self.label}"]
            df = pd.DataFrame(sample_ids_logs, columns=cols)
            df.to_parquet(f"{pipeline_base_path}/dnn_predictions/cd={self.DATE}/label={self.label}")

    def save(self, path):
        torch.save(self.model.state_dict(), path)

    def load(self, path):
        self.model.load_state_dict(torch.load(path))
        self.model.eval()