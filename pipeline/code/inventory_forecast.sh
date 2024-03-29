set -exu

DATE=$1
CODE=$2
aws s3 sync $CODE . --quiet

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --py-files config.py,path.py,util.py"

$SPARK forecasting/inventory_forecast/inventory_prediction.py $DATE
