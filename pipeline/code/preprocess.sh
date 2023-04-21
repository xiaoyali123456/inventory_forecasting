set -exu
DATE=$1
CODE=$2
aws s3 sync $CODE .

# debug begin
bash booking/server.sh &
sleep 3
# debug end

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0"

python3 fetch_requests.py $DATE
$SPARK check_new_match.py $DATE
