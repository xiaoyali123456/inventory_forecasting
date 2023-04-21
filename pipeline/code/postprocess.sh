set -exu
DATE=$1
CODE=$2
aws s3 sync $CODE .

bash booking/install.sh
bash booking/server.sh &
sleep 5

python3 postprocess.py $DATE
