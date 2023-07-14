import pandas as pd
import torch
import torchtext
from torch.utils.data import Dataset

from dnn_configuration import *

pd.options.mode.chained_assignment = None  # default='warn'


# generate vocabulary to map features to embedding index
def generate_vocabulary(df):
    vocabulary = {}
    for key in DNN_CONFIGURATION['used_features']:
        vocabulary[key] = torchtext.vocab.build_vocab_from_iterator(df[key], min_freq=1, max_tokens=100000,
                                                                    specials=['<unk>']).get_stoi()
    return vocabulary


# load match datasets, including training dataset and prediction dataset
class LiveMatchDataLoader(object):
    def __init__(self, train_dataset, prediction_dataset, label):
        vocabulary = generate_vocabulary(train_dataset)
        self.train_dataset = LiveMatchDataset(train_dataset, label, vocabulary, "train")
        self.prediction_dataset = LiveMatchDataset(prediction_dataset, label, vocabulary, "prediction")

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


# processing match dataset
class LiveMatchDataset(Dataset):
    def __init__(self, df, label, vocabulary, dataset_type):
        self.label = label
        self.vocabulary = vocabulary
        if dataset_type == "train":
            df = self.add_masked_data(df)
            df = df.loc[df[self.label] > 0]
        self.features, self.labels, self.sample_ids = self._parse(df)

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        x = [self.features[key][idx] for key in self.features]
        return x, self.labels[idx]

    def get_sample_ids(self):
        return self.sample_ids

    def add_masked_data(self, df):
        knock_off_df = df[df['match_stage'].isin([['semi-final'], ['final']])]

        knock_off_df['if_contain_india_team'] = knock_off_df['if_contain_india_team'].apply(lambda x: [UNKNOWN_TOKEN])
        knock_off_df['teams'] = knock_off_df['teams'].apply(lambda x: [UNKNOWN_TOKEN, UNKNOWN_TOKEN])
        knock_off_df['continents'] = knock_off_df['continents'].apply(lambda x: [DEFAULT_CONTINENT, DEFAULT_CONTINENT])

        df = pd.concat([df, knock_off_df])
        return df

    def _parse(self, df):
        features = {}
        for key in DNN_CONFIGURATION['used_features']:
            df[f"{key}_hots"] = df[key].apply(lambda x:
                                              [self.vocabulary[key][a] if a in self.vocabulary[key]
                                               else 0
                                               for a in x])
            features[key] = [list(val) for val in df[f"{key}_hots"]]

        # print(features)
        labels = [val for val in df[self.label]]

        sample_ids = [content_id for content_id in df['content_id']]
        # print(sample_ids)

        return features, labels, sample_ids
