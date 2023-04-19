set -exu
DATE=$1
CODE=$2
aws s3 sync $CODE .
# debug begin
bash test/server.sh &
sleep 3
# debug end
python3 postprocess.py $DATE
