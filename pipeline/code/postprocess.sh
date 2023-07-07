set -exu
DATE=$1
CODE=$2

aws s3 sync $CODE ~/CODE --quiet
cd ~/CODE

python3 -m pip install --user -r postprocess/requirements.txt
bash booking/server.sh &
sleep 5

python3 postprocess/postprocess.py $DATE
