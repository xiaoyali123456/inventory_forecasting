cd $(dirname $0)
pip install --user requirements.txt
uvicorn booking_api:app --reload --port 4321