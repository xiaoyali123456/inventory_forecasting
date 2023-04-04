set -ex

REGION=us-east-1
ENV=prod
SNS_TOPIC=arn:aws:sns:us-east-1:253474845919:sirius-notification
scriptFolder=$(dirname "$0")
pipeline_unique_name="live-ads-inventory-forecasting"

# find pipeline
pipeline_id=$(
    aws datapipeline list-pipelines --region $REGION --profile $ENV | python -c \
    "import sys, json; candidates = [x for x in json.loads(sys.stdin.read())['pipelineIdList'] \
    if x['name'] == '$pipeline_unique_name']; sys.stdout.write('' if 0 == len(candidates) else candidates[0]['id'])"
)

# destroy pipeline
if [[ -z $pipeline_id ]]; then
    echo "[INFO] pipeline $destroy_pipeline_name does not exist"
else
    echo "[INFO] deleting existed pipeline: $pipeline_id"
    aws datapipeline delete-pipeline --pipeline-id "$pipeline_id" --region $REGION --profile $ENV
    echo "[INFO] done: deleted pipeline: $pipeline_id"
fi

# create pipeline
new_pipeline_id=$(
  aws datapipeline create-pipeline --name "$pipeline_unique_name" --unique-id "$pipeline_unique_name" --region $REGION \
    --query pipelineId --output text --profile $ENV
)
aws datapipeline add-tags --pipeline-id "$new_pipeline_id" --region $REGION --profile $ENV \
  --tags key=Owner,value=tao.xiong@hotstar.com key=CostCenter,value=India key=Product,value=Hotstar key=Team,value=ML \
    key=Stage,value=prod key=Name,value=InterestBasedTargetingMlV3

# put pipeline definition
aws datapipeline put-pipeline-definition \
  --pipeline-id $new_pipeline_id \
  --pipeline-definition "file://$scriptFolder/datapipeline.json" \
  --region $REGION \
  --profile $ENV \
  --parameter-values \
    myStartDate=$START_DATE \
    mySubnetId=subnet-156c324f \
    myKeyPair=research-prod \
    myResourceRole=sirius_universal_role_prod \
    myRole=sirius_universal_role_prod \
    myRegion=$REGION \
    myEmrKeyPair=research-prod \
    myEmrRegion=$REGION \
    myEmrResourceRole=sirius_universal_role_prod \
    myEmrRole=sirius_universal_role_prod \
    myEmrSubnetId=subnet-156c324f \
    mySNSTopicArn=$SNS_TOPIC \
    mySNSTopicPdArn=arn:aws:sns:ap-southeast-1:084690408984:adtech_ml_pd

echo "[INFO] pipeline definition = $create_command"
eval "$create_command"
aws datapipeline activate-pipeline --pipeline-id "$new_pipeline_id" --region $REGION --profile $ENV


bash deploy_datapipeline_internal.sh
err=$?

echo "[INFO] send sns message"
if [ $err -ne 0 ]; then
  aws sns publish --topic-arn "$SNS_TOPIC" --subject "$pipeline_id" --message "data pipeline deployment failed!" --region $REGION
  exit 1
else
  aws sns publish --topic-arn "$SNS_TOPIC" --subject "$pipeline_id" --message "data pipeline deployment done!" --region $REGION
fi
