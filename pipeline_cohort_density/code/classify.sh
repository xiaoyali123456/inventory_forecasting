set -exu

DATE=$1
CODE=$2

aws s3 sync $CODE ~/CODE --quiet
cd ~/CODE

python3 -m pip install --user -r requirements.txt
python3 classify.py $DATE
