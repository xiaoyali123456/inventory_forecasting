DATE=$1
CODE=$2
aws s3 sync $CODE .

bash booking/server.sh &

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --py-files common.py"

$SPARK sampling/etl.py $DATE
$SPARK sampling/ewma.py $DATE
$SPARK sampling/combine.py $DATE
