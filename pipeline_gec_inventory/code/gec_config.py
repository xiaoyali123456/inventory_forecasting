# for gec
MAX_INT = 2147483647
VALID_SAMPLE_TAG = 1
STRING_SAMPLING_COLS = ['city', 'state', 'location_cluster',
                         'pincode',
                         'demo_gender', 'demo_age_range', 'demo_source',
                         'device_brand', 'device_model', 'device_carrier', 'device_network_data', 'device_platform', 'device_os_version', 'device_app_version',
                         'ibt',
                         'user_account_type',
                         'ad_placement',
                         'content_id', 'content_type', 'content_language',
                         'request_id', 'break_id',
                         'show_id', 'genre', 'title', 'channel', 'premium',
                         'custom_tags', 'user_segment']
VOD_SAMPLING_COLS = ['adv_id',
                     'city', 'state', 'location_cluster',
                     'gender', 'age_bucket',
                     'ibt', '3rd_party_cohorts',
                     'device_brand', 'device_model', 'device_carrier', 'device_platform', 'device_price',
                     'user_account_type',
                     'ad_placement',
                     'content_id', 'content_type', 'content_language',
                     'break_slot_count',
                     'show_id', 'genre', 'season_no', 'channel', 'premium',
                     'nccs']

BACKUP_SAMPLE_RATE = 0.25
ALL_ADPLACEMENT_SAMPLE_BUCKET = 100
VOD_SAMPLE_BUCKET = 300

# Q: why these values? A: there are all from shifu adplacements except audience platfrom
SUPPORTED_AD_PLACEMENT = ["BILLBOARD_HOME", "BILLBOARD_TV", "BILLBOARD_MOVIES", "BILLBOARD_NEWS",
                          "SKINNY_HOME", "SKINNY_TV", "SKINNY_MOVIES", "SKINNY_SPORTS",
                          "SKINNY_MULTIPLEX", "BTF_HOME", "BTF_TV", "BTF_MOVIES", "BTF_SPORTS",
                          "BTF_HOMELOW", "BTF_MOVIESLOW2", "PREROLL", "MIDROLL"]

SLACK_NOTIFICATION_TOPIC = "arn:aws:sns:us-east-1:253474845919:sirius-notification"
REGION = "us-east-1"

# git clone git@github.com:hotstar/live-ads-inventory-forecasting-ml.git
