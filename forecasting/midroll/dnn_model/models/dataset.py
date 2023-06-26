import pandas as pd
import torch
from torch.utils.data import Dataset
pd.options.mode.chained_assignment = None  # default='warn'


class LiveMatchDataLoader(object):
    def __init__(self, dataset, label_list, test_tournaments, if_mask_knock_off_matches, max_token):
        self.trainset = LiveMatchDataset(dataset, if_mask_knock_off_matches, max_token, label_list, removed_tournaments=test_tournaments+['ac2023', 'wc2023'])
        self.testset = LiveMatchDataset(dataset, if_mask_knock_off_matches, max_token, label_list, selected_tournaments=test_tournaments)

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
    def __init__(self, df, if_mask_knock_off_matches, max_token, label_list, selected_tournaments=None, removed_tournaments=None):
        self.label_list = label_list
        self.if_mask_knock_off_matches = if_mask_knock_off_matches
        self.max_token = max_token
        self.features, self.labels, self.sample_ids = self._parse(df, selected_tournaments, removed_tournaments)

    def __len__(self):
        return len(self.labels[self.label_list[0]])

    def __getitem__(self, idx):
        x = [self.features[key][idx] for key in self.features]
        return x, [self.labels[label][idx] for label in self.label_list]

    def get_sample_ids(self):
        return self.sample_ids

    def mask_data(self, df):
        df['if_contain_india_team_hots'] = [self.max_token - 1]
        df['teams_hots'] = [self.max_token - 2, self.max_token - 1]
        # print(df['teams_hots'])
        df['continents_hots'] = [self.max_token - 2, self.max_token - 1]
        return df

    def _parse(self, df, selected_tournaments, removed_tournaments):
        feature_config = [
            'vod_type_hots',
            'match_stage_hots',
            'tournament_name_hots',
            'match_type_hots',
            'if_contain_india_team_hots',
            'if_holiday_hots',
            'match_time_hots',
            'if_weekend_hots',
            'tournament_type_hots',
            'teams_hots',
            'continents_hots',
            'teams_tier_hots',
        ]
        # for test dataset
        if selected_tournaments is not None:
            df = df.loc[df['tournament'].isin(selected_tournaments)]
            # print(len(df))
            if self.if_mask_knock_off_matches:
                mask_df = df[df['match_stage'].isin(['semi-final', 'final'])]
                mask_df = self.mask_data(mask_df)
                # print(len(mask_df))
                df = pd.concat([df[~df['match_stage'].isin(['semi-final', 'final'])], mask_df])
                print(len(df))

        # for train dataset
        if removed_tournaments is not None:
            # df = df.loc[~df['tournament'].isin(['ac2023', 'wc2023'])]
            df = df.loc[~df['tournament'].isin(removed_tournaments)]
            df = df.loc[df[self.label_list[0]] > 0]
            # print(len(df))
            # if self.if_mask_knock_off_matches:
            mask_df = df[df['match_stage'].isin(['semi-final', 'final'])]
            mask_df = self.mask_data(mask_df)
            # # print(len(mask_df))
            df = pd.concat([df, mask_df])
            # print(len(df))

        print(len(df))
        features = {}
        for key in feature_config:
            rawlist = [val for val in df[key]]
            # print(rawlist[0])
            # print(len(rawlist[0]))
            # print(rawlist[0][1:-1])
            # print(rawlist[0][1:-1].split(','))
            # print([int(v) for v in rawlist[0][1:-1].split(',')])
            # features[key] = [[int(v) for v in val[1:-1].split(',')] for val in rawlist]
            #print(key, len(features[key]), features[key][:16])
            features[key] = [list(val) for val in df[key]]

        # labels = [val for val in df['frees_watching_match_rate']]
        labels = {}
        for label in self.label_list:
            labels[label] = [val for val in df[label]]
            #print('label', len(labels), labels)

        # names = df['tournament'] +'|'+ df['title'] +'|'+ df['vod_type'].map(str) +'|'+ df['match_type'].map(str)
        # names = df['tournament'] +'|'+ df['title']
        names = df['content_id']
        # names = df['content_id'] +' | '+ df['title']
        sample_ids = [name for name in names]

        return features, labels, sample_ids









