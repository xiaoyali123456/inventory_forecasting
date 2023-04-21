set -exu

cd $(dirname $0)
python3 -m uvicorn booking_api:app --reload --port 4321
