set -exu

DATE=$1
CODE=$2
aws s3 sync $CODE .

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0"

$SPARK feature.py $DATE
# disable due to no new data yet
# $SPARK label.py
# $SPARK xgb_model.py
# $SPARK inventory_prediction.py
