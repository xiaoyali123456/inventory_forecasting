# Pytorch network api

import torch
from torch import nn

from dnn_configuration import *


class DeepEmbMLP(nn.Module):
    def __init__(self, column_num):
        torch.manual_seed(seed=12345)
        super().__init__()
        emb_size = DNN_CONFIGURATION['embedding_table_size']
        emb_dim = DNN_CONFIGURATION['embedding_dim']
        # self.pooling_way = "average"
        self.pooling_way = "concat"
        if self.pooling_way == "average":
            self.mlp = nn.Sequential(
                nn.Linear(column_num*emb_dim, DNN_CONFIGURATION['mlp_layer_sizes'][0]),  # there are 3 two-hots vectors
                nn.ReLU(),
                nn.Linear(DNN_CONFIGURATION['mlp_layer_sizes'][0], DNN_CONFIGURATION['mlp_layer_sizes'][1]),
                nn.ReLU(),
                nn.Linear(DNN_CONFIGURATION['mlp_layer_sizes'][1], 1),
            )
        else:
            self.mlp = nn.Sequential(
                nn.Linear((column_num + 3) * emb_dim, DNN_CONFIGURATION['mlp_layer_sizes'][0]),
                # there are 3 two-hots vectors
                nn.ReLU(),
                nn.Linear(DNN_CONFIGURATION['mlp_layer_sizes'][0], DNN_CONFIGURATION['mlp_layer_sizes'][1]),
                nn.ReLU(),
                nn.Linear(DNN_CONFIGURATION['mlp_layer_sizes'][1], 1),
            )
        self.encoder = [nn.Embedding(emb_size, emb_dim) for i in range(column_num)]
        for emb in self.encoder:
            nn.init.trunc_normal_(emb.weight.data)

    def embedding_lookup(self, x):
        input_layer = []
        for i, seq_fea in enumerate(x):
            xembs = [self.encoder[i](fea) for fea in seq_fea]
            # print(xembs.shape)
            if self.pooling_way == "average":
                input_layer.append(torch.mean(torch.stack(xembs), dim=0))
            else:
                input_layer.extend(xembs)
        print(len(input_layer))
        return input_layer

    def forward(self, x):
        input_layer = self.embedding_lookup(x)
        x = torch.cat(input_layer, dim=-1)
        p = self.mlp(x)
        return p.squeeze()