set -exu

cd=$1

python3 fetch_requests.py $cd
python3 check_new_match.py $cd
