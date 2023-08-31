set -exu

DATE=$1
CODE=$2
aws s3 sync $CODE . --quiet

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --conf spark.driver.memory=8g \
    --conf spark.driver.maxResultSize=0 \
    --py-files config.py,path.py,util.py"

$SPARK forecasting/dataset_update/dau_update.py $DATE
$SPARK forecasting/dataset_update/dataset_update.py $DATE