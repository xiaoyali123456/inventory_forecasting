import requests
from common import SERVER_URL_ROOT

if __name__ == '__main__':
    r = requests.patch(
        SERVER_URL_ROOT + 'inventory/123_586/ad-placement/MIDROLL/forecast-request',
        json = {
            "request_status": "SUCCESS",
            "version": 1,
        }
    )
    print('updated status:', r.status_code)
