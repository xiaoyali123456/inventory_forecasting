# for gec
# used for adv_id(uid) string hash
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

# 1. adv_id % 4 == 1   => setA
# 2. adv_id % 100 == 1 on setA => setB
# 3. adv_id % 300 == 1 on setB => set(PREROLL,MIDROLL)
BACKUP_SAMPLE_RATE = 0.25
ALL_ADPLACEMENT_SAMPLE_BUCKET = 100
VOD_SAMPLE_BUCKET = 300

# Q: why these values? A: there are all from shifu adplacements except audience platfrom
SUPPORTED_AD_PLACEMENT = ["BILLBOARD_HOME", "BILLBOARD_TV", "BILLBOARD_MOVIES", "BILLBOARD_NEWS",
                          "SKINNY_HOME", "SKINNY_TV", "SKINNY_MOVIES", "SKINNY_SPORTS",
                          "SKINNY_MULTIPLEX", "BTF_HOME", "BTF_TV", "BTF_MOVIES", "BTF_SPORTS",
                          "BTF_HOMELOW", "BTF_MOVIESLOW2", "PREROLL", "MIDROLL"]

# hotstar-dev sns
SLACK_NOTIFICATION_TOPIC = "arn:aws:sns:us-east-1:253474845919:sirius-notification"
REGION = "us-east-1"


# prophet config:
PROPHET_MODEL_CONFIG = {
    "OTHERS": {
        'changepoint_prior_scale': 0.01,
        'holidays_prior_scale': 10,
        'yearly_seasonality': False,
        'weekly_seasonality': 'auto'  # Q: why auto? what is auto? A: can be True, no too much difference
    },
    "MIDROLL": {
        'changepoint_prior_scale': 0.01,
        'holidays_prior_scale': 10,
        'yearly_seasonality': False,
        'weekly_seasonality': True
    },
    "BILLBOARD_HOME": {
        'changepoint_prior_scale': 0.01,
        'holidays_prior_scale': 10,
        'yearly_seasonality': False,
        'weekly_seasonality': 'auto'
    },
    "SKINNY_HOME": {
        'changepoint_prior_scale': 0.01,
        'holidays_prior_scale': 10,
        'yearly_seasonality': False,
        'weekly_seasonality': 'auto'
    },
    "PREROLL": {
        'changepoint_prior_scale': 0.01,
        'holidays_prior_scale': 10,
        'yearly_seasonality': False,
        'weekly_seasonality': True
    },
    'TEST_PERIOD': 90,
    'PREDICTION_PERIOD': 365
}

# git clone git@github.com:hotstar/live-ads-inventory-forecasting-ml.git
