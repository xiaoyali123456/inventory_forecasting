set -exu
DATE=$1
CODE=$2
aws s3 sync $CODE .
bash test/run.sh &
sleep 3
python3 postprocess.py $DATE
