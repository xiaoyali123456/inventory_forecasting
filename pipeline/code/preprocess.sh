set -exu

DATE=$1
CODE=$2
aws s3 sync $CODE .

bash test/run.sh &

SPARK="spark-submit --deploy-mode client \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0"

python3 fetch_requests.py $DATE
$SPARK check_new_match.py $DATE
