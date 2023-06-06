
import torch
from torch import nn


class DeepEmbMLP(nn.Module):
    def __init__(self, columns, max_token, emb_dim=6, num_task=1):
        torch.manual_seed(12345)
        super().__init__()
        self.emb_dim = emb_dim
        self.mlp = nn.Sequential(
            nn.Linear(15*emb_dim, 64),
            nn.ReLU(),
            nn.Linear(64, 64),
            nn.ReLU(),
            nn.Linear(64, num_task),
        )
        self.encoder = [nn.Embedding(max_token, emb_dim) for i in range(columns)]
        for emb in self.encoder:
            # nn.init.normal_(emb.weight, mean=0, std=0.5)
            # nn.init.xavier_uniform_(emb.weight.data)
            nn.init.trunc_normal_(emb.weight.data)

    def embedding_lookup(self, x):
        input_layer = []
        for i, seq_fea in enumerate(x):
            #print(len(seq_fea))
            xembs = [self.encoder[i](fea) for fea in seq_fea]
            #print(i, xembs)
            input_layer.extend(xembs)
        return input_layer

    def forward(self, x):
        input_layer = self.embedding_lookup(x)
        x = torch.cat(input_layer, dim=-1)
        p = self.mlp(x)
        return p.squeeze()