set -exu
DATE=$1
CODE=$2
aws s3 sync $CODE . --quiet

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --conf spark.driver.memory=8g \
    --conf spark.driver.maxResultSize=0 \
    --py-files config.py,path.py,util.py"

$SPARK sampling/preroll/etl.py $DATE
#$SPARK sampling/preroll/ewma.py $DATE
#$SPARK sampling/preroll/combine.py $DATE

SLACK_NOTIFICATION_TOPIC="arn:aws:sns:us-east-1:253474845919:sirius-notification"
REGION="us-east-1"
aws sns publish --topic-arn "$SLACK_NOTIFICATION_TOPIC" --subject "preroll inventory forecasting" --message "sampling done" --region $REGION
