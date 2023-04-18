cd $(dirname $0)
python3 -m pip install --user -r requirements.txt
uvicorn booking_api:app --reload --port 4321