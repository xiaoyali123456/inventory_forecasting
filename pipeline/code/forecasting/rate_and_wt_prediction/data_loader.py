import pandas as pd
import torch
import torchtext
from torch.utils.data import Dataset
from dnn_configuration import *

pd.options.mode.chained_assignment = None  # default='warn'


def generate_feature_mapping(df):
    feature_mapping = {}
    for key in dnn_configuration['used_features']:
        feature_mapping[key] = torchtext.vocab.build_vocab_from_iterator(df[key], min_freq=1, max_tokens=100000, specials=['<unk>']).get_stoi()
    return feature_mapping


class LiveMatchDataLoader(object):
    def __init__(self, train_dataset, prediction_dataset, label):
        feature_mapping = generate_feature_mapping(train_dataset)
        self.train_dataset = LiveMatchDataset(train_dataset, label, feature_mapping)
        self.prediction_dataset = LiveMatchDataset(prediction_dataset, label, feature_mapping)

    def get_dataset(self, batch_size, mode='train'):
        if mode == 'train':
            return torch.utils.data.DataLoader(dataset=self.train_dataset,
                                               batch_size=batch_size,
                                               shuffle=True)
        else:
            return torch.utils.data.DataLoader(dataset=self.prediction_dataset,
                                               batch_size=batch_size,
                                               shuffle=False)

    def get_sample_ids(self, mode):
        if mode == 'train':
            return self.train_dataset.get_sample_ids()
        else:
            return self.prediction_dataset.get_sample_ids()


class LiveMatchDataset(Dataset):
    def __init__(self, df, label, feature_mapping):
        self.label = label
        self.feature_mapping = feature_mapping
        self.features, self.labels, self.sample_ids = self._parse(df)

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        x = [self.features[key][idx] for key in self.features]
        return x, self.labels[idx]

    def get_sample_ids(self):
        return self.sample_ids

    def _parse(self, df):
        features = {}
        for key in dnn_configuration['used_features']:
            df[f"{key}_hots"] = df[key].apply(lambda x: [self.feature_mapping[key][a] if a in self.feature_mapping[key] else 0 for a in x])
            features[key] = [list(val) for val in df[f"{key}_hots"]]

        # print(features)
        labels = [val for val in df[self.label]]

        sample_ids = [content_id for content_id in df['content_id']]
        # print(sample_ids)

        return features, labels, sample_ids









