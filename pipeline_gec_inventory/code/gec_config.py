# for gec
MAX_INT = 2147483647
VALID_SAMPLE_TAG = 1
SAMPLING_COLS = ['content_id', 'city', 'state', 'location_cluster', 'pincode',
                 'gender', 'age_bucket', 'ibt', 'device_brand', 'device_model', 'device_carrier', 'device_network_data',
                 'user_account_type', 'device_platform', 'device_os_version', 'device_app_version',
                 'ad_placement', 'content_type', 'content_language', 'demo_age_range', 'custom_tags', 'user_segment',
                 'show_id', 'genre', 'season_no', 'title', 'channel', 'premium']
VOD_SAMPLING_COLS = ['content_id', 'adv_id', 'city', 'state', 'location_cluster', 'gender', 'age_bucket',
            'ibt', 'device_brand', 'device_model', 'device_carrier', 'user_account_type',
            'device_platform', 'ad_placement', 'content_type', 'content_language', 'break_slot_count',
            'show_id', 'genre', 'season_no', 'channel', 'premium', 'nccs', 'device_price', '3rd_party_cohorts']


BACKUP_SAMPLE_RATE = 0.25
ALL_ADPLACEMENT_SAMPLE_BUCKET = 100
VOD_SAMPLE_BUCKET = 300

SUPPORTED_AD_PLACEMENT = ["BILLBOARD_HOME", "BILLBOARD_TV", "BILLBOARD_MOVIES", "BILLBOARD_NEWS",
                          "SKINNY_HOME", "SKINNY_TV", "SKINNY_MOVIES", "SKINNY_SPORTS",
                          "SKINNY_MULTIPLEX", "BTF_HOME", "BTF_TV", "BTF_MOVIES", "BTF_SPORTS",
                          "BTF_HOMELOW", "BTF_MOVIESLOW2", "PREROLL", "MIDROLL"]

SLACK_NOTIFICATION_TOPIC = "arn:aws:sns:us-east-1:253474845919:sirius-notification"
REGION = "us-east-1"

# git clone git@github.com:hotstar/live-ads-inventory-forecasting-ml.git

