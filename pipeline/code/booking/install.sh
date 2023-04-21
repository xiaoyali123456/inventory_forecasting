set -exu

cd $(dirname $0)
python3 -m pip install --upgrade pip
python3 -m pip install --user -r requirements.txt

