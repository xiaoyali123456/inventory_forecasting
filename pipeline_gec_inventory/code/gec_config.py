# for gec
MAX_INT = 2147483647
VALID_SAMPLE_TAG = 1
SAMPLING_COLS = ['content_id', 'city', 'state', 'location_cluster', 'pincode',
                 'gender', 'age_bucket', 'ibt', 'device_brand', 'device_model', 'device_carrier', 'device_network_data',
                 'user_account_type', 'device_platform', 'device_os_version', 'device_app_version',
                 'ad_placement', 'content_type', 'content_language']
CMS_COLS = ['show_id', 'genre', 'season_no', 'title']
FCAP_RANGE = [i for i in range(1, 11)]


supported_ad_placement = ["BILLBOARD_HOME", "BILLBOARD_TV", "BILLBOARD_MOVIES", "BILLBOARD_NEWS",
                          "SKINNY_HOME", "SKINNY_TV", "SKINNY_MOVIES", "SKINNY_SPORTS",
                          "SKINNY_MULTIPLEX", "BTF_HOME", "BTF_TV", "BTF_MOVIES", "BTF_SPORTS",
                          "BTF_HOMELOW", "BTF_MOVIESLOW2", "PREROLL", "MIDROLL"]

SLACK_NOTIFICATION_TOPIC = "arn:aws:sns:us-east-1:253474845919:sirius-notification"
REGION = "us-east-1"

# git clone git@github.com:hotstar/live-ads-inventory-forecasting-ml.git

