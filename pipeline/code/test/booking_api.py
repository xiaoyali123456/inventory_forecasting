import json
from enum import Enum, auto

from fastapi import FastAPI, Query
from typing_extensions import Annotated
from pydantic import BaseModel

class Status(str, Enum):
    INIT = 'INIT'
    IN_PROGRESS = 'IN_PROGRESS'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'

class AdPlacement(str, Enum):
    MIDROLL = 'MIDROLL'
    PREROLL = 'PREROLL'

class Payload(BaseModel):
    request_status: Status
    version: int

app = FastAPI()
example = 'example/requests.json'

@app.get('/inventory/forecast-request')
def get_by_status(
    status: str = None,
    page_number: Annotated[int, Query(alias='page-number')] = None,
    page_size: Annotated[int, Query(alias='page-size')] = None,
    ):
    print(locals())
    with open(example) as f: 
        return json.load(f)

# TODO: what is ad_placement?
@app.patch('/inventory/{inventory_id}/ad-placement/{ad_placement}/forecast-request')
def update_req_status(
    inventory_id: str,
    ad_placement: AdPlacement,
    data: Payload
    ):
    print(locals())
    with open(example) as f: 
        return json.load(f)
