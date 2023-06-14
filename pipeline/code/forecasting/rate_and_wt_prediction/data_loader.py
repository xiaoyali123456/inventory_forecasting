import pandas as pd
import torch
from torch.utils.data import Dataset
from dnn_configuration import *

pd.options.mode.chained_assignment = None  # default='warn'


class LiveMatchDataLoader(object):
    def __init__(self, train_dataset, test_dataset, label):
        self.trainset = LiveMatchDataset(train_dataset, label)
        self.testset = LiveMatchDataset(test_dataset, label)

    def get_dataset(self, batch_size, mode='train'):
        if mode == 'train':
            return torch.utils.data.DataLoader(dataset=self.trainset,
                                               batch_size=batch_size,
                                               shuffle=True)
        else:
            return torch.utils.data.DataLoader(dataset=self.testset,
                                               batch_size=batch_size,
                                               shuffle=False)

    def get_sample_ids(self, mode):
        if mode == 'train':
            return self.trainset.get_sample_ids()
        else:
            return self.testset.get_sample_ids()


class LiveMatchDataset(Dataset):
    def __init__(self, df, label):
        self.label = label
        self.features, self.labels, self.sample_ids = self._parse(df)

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        x = [self.features[key][idx] for key in self.features]
        return x, self.labels

    def get_sample_ids(self):
        return self.sample_ids

    def _parse(self, df):
        features = {}
        for key in dnn_configuration['used_features']:
            rawlist = [val for val in df[key]]
            features[key] = [[int(v) for v in val[1:-1].split(',')] for val in rawlist]

        labels = [val for val in df[self.label]]

        names = df['content_id']
        sample_ids = [name for name in names]

        return features, labels, sample_ids









