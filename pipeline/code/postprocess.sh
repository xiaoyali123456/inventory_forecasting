set -exu
DATE=$1
CODE=$2

aws s3 sync $CODE ~/CODE --quiet
cd ~/CODE

SLACK_NOTIFICATION_TOPIC="arn:aws:sns:us-east-1:253474845919:sirius-notification"
REGION="us-east-1"

python3 -m pip install --user -r postprocess/requirements.txt
#bash booking/server.sh &
#sleep 5
aws sns publish --topic-arn "$SLACK_NOTIFICATION_TOPIC" --subject "midroll inventory forecasting" --message "postprocess packages installed done" --region $REGION


#curl http://adtech-inventory-booking-service-alb-0-int.internal.sgp.hotstar.com/api/v1/inventory/forecast-request\?page-size\=10\&page-number\=0


SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --conf spark.driver.memory=8g \
    --conf spark.driver.maxResultSize=0 \
    --py-files config.py,path.py,util.py"

$SPARK postprocess/combine.py $DATE

PYTHONPATH="$PWD" python3 postprocess/postprocess.py $DATE

aws sns publish --topic-arn "$SLACK_NOTIFICATION_TOPIC" --subject "midroll inventory forecasting" --message "postprocess done" --region $REGION
