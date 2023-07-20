set -exu
DATE=$1
CODE=$2
aws s3 sync $CODE . --quiet

#bash booking/server.sh &
#sleep 5

#curl http://adtech-inventory-booking-service-alb-0-int.internal.sgp.hotstar.com/api/v1/inventory/forecast-request\?page-size\=10\&page-number\=0


SLACK_NOTIFICATION_TOPIC="arn:aws:sns:us-east-1:253474845919:sirius-notification"
REGION="us-east-1"
#aws sns publish --topic-arn "$SLACK_NOTIFICATION_TOPIC" --subject "midroll inventory forecasting" --message "booking tool server starts" --region $REGION

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --py-files common.py"

$SPARK preprocess/fetch_requests.py $DATE || sleep 100m
aws sns publish --topic-arn "$SLACK_NOTIFICATION_TOPIC" --subject "midroll inventory forecasting" --message "fetch_requests on $DATE done" --region $REGION

$SPARK preprocess/fetch_match_cms.py $DATE
aws sns publish --topic-arn "$SLACK_NOTIFICATION_TOPIC" --subject "midroll inventory forecasting" --message "fetch_match_cms $DATE done" --region $REGION
