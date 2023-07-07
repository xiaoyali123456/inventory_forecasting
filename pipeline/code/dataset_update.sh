set -exu

DATE=$1
CODE=$2
aws s3 sync $CODE . --quiet

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --py-files common.py,config.py,path.py,util.py"

$SPARK midroll/dataset_update/dau_update.py $DATE
$SPARK midroll/dataset_update/dataset_update.py $DATE