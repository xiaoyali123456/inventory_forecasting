set -exu

cd $(dirname $0)
python3 -m pip install --upgrade pip
python3 -m pip install --user -r requirements.txt
python3 -m uvicorn booking_api:app --reload --port 4321
