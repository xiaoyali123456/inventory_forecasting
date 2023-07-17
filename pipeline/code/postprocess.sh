set -exu
DATE=$1
CODE=$2

aws s3 sync $CODE ~/CODE --quiet
cd ~/CODE

python3 -m pip install --user -r postprocess/requirements.txt
bash booking/server.sh &
sleep 5

PYTHONPATH="$PWD" python3 postprocess/postprocess.py $DATE


SLACK_NOTIFICATION_TOPIC="arn:aws:sns:us-east-1:253474845919:sirius-notification"
REGION="us-east-1"
aws sns publish --topic-arn "$SLACK_NOTIFICATION_TOPIC" --subject "midroll inventory forecasting" --message "postprocess done" --region $REGION
