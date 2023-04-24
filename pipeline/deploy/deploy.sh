set -ex

# show all environment variables
if [[ -z $START_DATE || $START_DATE == "default" ]]; then
  START_DATE=$(date -I)
fi
echo $DESTROY $START_DATE $CODE_ONLY

# PROFILE="--profile $ENV" # comment out for debug
ENV=prod
REGION=us-east-1
PIPELINE_UNIQUE_NAME=live-ads-inventory-forecasting
S3_PIPELINE_DIR=s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/pipeline
SNS_TOPIC=arn:aws:sns:us-east-1:253474845919:sirius-notification

# deploy code
LOCAL_DIR=$(cd $(dirname "$0")/.. && pwd)
aws s3 sync --exclude 'logs/*' --delete $LOCAL_DIR $S3_PIPELINE_DIR
if [[ "$CODE_ONLY" == true ]]; then
  exit
fi
# exit

# find pipeline
res=$(
  aws datapipeline list-pipelines --region $REGION $PROFILE --query "pipelineIdList[?name == '$PIPELINE_UNIQUE_NAME'].id|[0] || ''"
)
pipeline_id="${res//\"/}"

# destroy pipeline
if [[ -z "$pipeline_id" ]]; then
  echo "[INFO] pipeline $destroy_pipeline_name does not exist"
else
  aws datapipeline delete-pipeline --pipeline-id "$pipeline_id" --region $REGION $PROFILE
  echo "[INFO] deleted pipeline: $pipeline_id"
fi
if [[ "$DESTROY" == true ]]; then
  exit
fi

# create new pipeline
new_pipeline_id=$(
  aws datapipeline create-pipeline --name "$PIPELINE_UNIQUE_NAME" --unique-id "$PIPELINE_UNIQUE_NAME" \
    --region $REGION $PROFILE --query pipelineId --output text
)
aws datapipeline add-tags --pipeline-id "$new_pipeline_id" --region $REGION $PROFILE \
  --tags key=Owner,value=tao.xiong@hotstar.com key=CostCenter,value=India key=Product,value=Hotstar key=Team,value=ML \
  key=Stage,value=prod key=Name,value=$PIPELINE_UNIQUE_NAME

aws datapipeline put-pipeline-definition \
  --pipeline-id $new_pipeline_id \
  --pipeline-definition "file://$LOCAL_DIR/deploy/datapipeline.json" \
  --region $REGION $PROFILE \
  --parameter-values \
  myProjectFolder=$S3_PIPELINE_DIR \
  myStartDate=$START_DATE

aws datapipeline activate-pipeline --pipeline-id "$new_pipeline_id" --region $REGION $PROFILE
aws sns publish --topic-arn "$SNS_TOPIC" --subject "$new_pipeline_id" --message "datapipeline $PIPELINE_UNIQUE_NAME deployment done!" --region $REGION
