import requests
from common import BOOKING_TOOL_URL

if __name__ == '__main__':
    r = requests.patch(
        BOOKING_TOOL_URL + 'inventory/123_586/ad-placement/MIDROLL/forecast-request',
        json = {
            "request_status": "SUCCESS",
            "version": 1,
        }
    )
    print('updated status:', r.status_code)
