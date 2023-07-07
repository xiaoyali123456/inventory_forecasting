set -exu

DATE=$1
CODE=$2
aws s3 sync $CODE .

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --py-files common.py,config.py,path.py,util.py"

$SPARK midroll/inventory_forecast/inventory_prediction.py $DATE
