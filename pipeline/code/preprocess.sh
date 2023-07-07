set -exu
DATE=$1
CODE=$2
aws s3 sync $CODE .

bash booking/server.sh &
sleep 5

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --py-files common.py"

$SPARK preprocess/fetch_requests.py $DATE
$SPARK preprocess/fetch_match_cms.py $DATE
