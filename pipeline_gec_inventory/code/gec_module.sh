set -exu

DATE=$1
CODE=$2
aws s3 sync $CODE . --quiet

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --conf spark.driver.memory=8g \
    --conf spark.driver.maxResultSize=0 \
    --py-files gec_config.py,gec_path.py,gec_util.py"

$SPARK sampling/sampling.py $DATE
$SPARK index_building/index_building.py $DATE

$SPARK inventory_prediction/prediction.py $DATE


