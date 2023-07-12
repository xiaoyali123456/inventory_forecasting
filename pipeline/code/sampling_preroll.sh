DATE=$1
CODE=$2
aws s3 sync $CODE . --quiet

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --py-files common.py"

$SPARK sampling/preroll/etl.py $DATE
