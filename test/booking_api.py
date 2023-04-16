import json
from typing import Optional

from fastapi import FastAPI, Query
from typing_extensions import Annotated

app = FastAPI()

@app.get('/inventory/forecast-request')
def get_by_status(
    status: str = None,
    page_number: Annotated[int, Query(alias='page-number')] = None,
    page_size: Annotated[int, Query(alias='page-size')] = None,
    ):
    print(locals())
    with open('example/requests.json') as f: 
        return json.load(f)

@app.patch('/inventory/{inventory_id}/ad-placement/{ad_placement}/forecast-request')
def update_req_status(
    inventory_id: str,
    ad_placement: str,
    request_status: str = None,
    version: int = 1,
    ):
    print(locals())
    with open('req.json') as f: 
        return json.load(f)


