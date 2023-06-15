pipeline_base_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline"

slack_notification_topic = "arn:aws:sns:us-east-1:253474845919:sirius-notification"
region = "us-east-1"

free_rate_label = "frees_watching_match_rate"
free_wt_label = "watch_time_per_free_per_match"
sub_rate_label = "subscribers_watching_match_rate"
sub_wt_label = "watch_time_per_subscriber_per_match"
label_list = [free_rate_label, free_wt_label, sub_rate_label, sub_wt_label]


dnn_configuration = {
    'used_features': [
            'vod_type_hots',
            'match_stage_hots',
            'tournament_name_hots',
            'match_type_hots',
            'if_contain_india_team_hots',
            'if_holiday_hots',
            'match_time_hots',
            'if_weekend_hots',
            'tournament_type_hots',
            'teams_hots',
            'continents_hots',
            'teams_tier_hots',
        ],
    'train_batch_size': 16,
    'test_batch_size': 64,
    'epoch_num': 30,
    'lr': 5e-3,
    'weight_decay': 1e-3,
    'embedding_table_size': 100,
    'embedding_dim': 6,
    'mlp_layer_sizes': [64, 64]
}

huber_loss_parameter_dic = {
    free_rate_label: 0.1,
    free_wt_label: 1,
    sub_rate_label: 0.1,
    sub_wt_label: 1
}