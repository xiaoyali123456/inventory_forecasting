import gspread
import os
import pandas as pd

os.system('aws s3 cp s3://adtech-ml-perf-ads-us-east-1-prod-v1/live_inventory_forecasting/data/minliang.lin@hotstar.com-service-account.json .')
gc = gspread.service_account('minliang.lin@hotstar.com-service-account.json')
x = gc.open('live_inventory_forecasting_cms')
df = pd.DataFrame(x.sheet1.get_all_records())
