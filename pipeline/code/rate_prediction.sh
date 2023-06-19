#!/bin/bash

DATE=$1
CODE=$2

source activate pytorch

aws s3 sync $CODE .

# shellcheck disable=SC2164
cd forecasting/rate_and_wt_prediction

python3 main.py $DATE
