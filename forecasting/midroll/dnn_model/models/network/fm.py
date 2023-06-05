import torch
from torch import nn


class FM(nn.Module):
    def __init__(self):
        super(FM, self).__init__()

    def forward(self, inputs):
        x = inputs  # [B * F * D]
        square_of_sum = torch.sum(x, dim=1, keepdim=False)**2
        #print(square_of_sum.shape)
        sum_of_square = torch.sum(x**2, dim=1, keepdim=False)
        #print(sum_of_square.shape)
        cross_term = square_of_sum - sum_of_square
        cross_term = 0.5* torch.sum(cross_term, dim=1, keepdim=True)
        #print(cross_term.shape)
        return cross_term
