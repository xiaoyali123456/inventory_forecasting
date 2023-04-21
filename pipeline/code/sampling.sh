DATE=$1
CODE=$2
aws s3 sync $CODE .

# debug begin
bash booking/server.sh &
sleep 3
# debug end

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --py-files common.py"

$SPARK sampling/etl.py $DATE
