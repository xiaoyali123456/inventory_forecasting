DATE=$1
CODE=$2

echo "$DATE"

aws sns publish --topic-arn "arn:aws:sns:us-east-1:253474845919:sirius-notification" --subject "midroll rate forecasting" --message "starts" --region "us-east-1"


source activate pytorch

aws s3 sync $CODE .

# shellcheck disable=SC2164
cd forecasting/rate_and_wt_prediction

python3 main.py $DATE
