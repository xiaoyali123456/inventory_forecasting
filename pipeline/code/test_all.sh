set -exu

DATE="2023-05-22"
CODE="s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/pipeline/code/"
SPARK="spark-submit --deploy-mode client \
    --conf spark.dynamicAllocation.enabled=true \
    --packages org.apache.hudi:hudi-spark-bundle_2.11:0.9.0 \
    --py-files common.py,forecasting/path.py,forecasting/config.py,forecasting/util.py"

#aws s3 sync $(dirname $0) $CODE
#aws s3 sync $CODE .

bash booking/server.sh &
sleep 5

# preprocess
# python3 fetch_requests.py $DATE
# $SPARK check_matches_snapshot.py $DATE

# forecasting
# $SPARK forecasting/active_user_etl_and_predict.py $DATE

# $SPARK forecasting/feature.py $DATE
$SPARK forecasting/xgb_model.py $DATE
$SPARK forecasting/inventory_prediction.py $DATE

#$SPARK sampling/ewma.py $DATE


#######
# test non prod env

# post
curl https://adtech-blaze-gateway-pp.pp.hotstar-labs.com/inventory-booking-service-eks/api/v1/inventory/forecast-request -XPOST -H 'content-type:application/json' -d @/home/hadoop/minliang/live/pipeline/code/booking/example/post2.json

# get
curl https://adtech-blaze-gateway-pp.pp.hotstar-labs.com/inventory-booking-service-eks/api/v1/inventory/forecast-request\?page-size\=10\&page-number\=0
curl https://adtech-blaze-gateway-pp.pp.hotstar-labs.com/inventory-booking-service-eks/api/v1/inventory/forecast-request\?page-size\=10\&page-number\=0\&status\=INIT

curl 'https://adtech-blaze-gateway-pp.pp.hotstar-labs.com/inventory-booking-service-eks/api/v1/inventory/forecast-request?page-size=10&page-number=0&status=INIT'

# set status to success
curl https://adtech-blaze-gateway-pp.pp.hotstar-labs.com/inventory-booking-service-eks/api/v1/inventory/111_222/ad-placement/MIDROLL/forecast-request -XPATCH -H 'content-type:application/json' -d '{ "request_status":"IN_PROGRESS","version":1}'


curl https://adtech-blaze-gateway-pp.pp.hotstar-labs.com/inventory-booking-service-eks/api/v1/inventory/111_222/ad-placement/MIDROLL/forecast-request -XPATCH -H 'content-type:application/json' -d '{ "requestStatus":"SUCCESS","version":1}'
curl https://adtech-blaze-gateway-pp.pp.hotstar-labs.com/inventory-booking-service-eks/api/v1/inventory/112_222/ad-placement/MIDROLL/forecast-request -XPATCH -H 'content-type:application/json' -d '{ "requestStatus":"SUCCESS","version":2}'
