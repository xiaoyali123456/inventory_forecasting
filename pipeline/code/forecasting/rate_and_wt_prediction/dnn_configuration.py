# Dnn configuration

pipeline_base_path = "s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline"
training_data_path = f"{pipeline_base_path}/all_features_hots_format_full_avod_and_simple_one_hot_overall_with_reach_rate"
prediction_feature_path = f"{pipeline_base_path}/prediction/all_features_hots_format"
train_match_table_path = f"{pipeline_base_path}/match_table/train"
prediction_match_table_path = f"{pipeline_base_path}/match_table/prediction"

slack_notification_topic = "arn:aws:sns:us-east-1:253474845919:sirius-notification"
region = "us-east-1"

free_rate_label = "frees_watching_match_rate"
free_wt_label = "watch_time_per_free_per_match"
sub_rate_label = "subscribers_watching_match_rate"
sub_wt_label = "watch_time_per_subscriber_per_match"
reach_rate_label = "reach_rate"
label_list = [free_rate_label, free_wt_label, sub_rate_label, sub_wt_label, reach_rate_label]


dnn_configuration = {
    'used_features': [
            'vod_type',
            'match_stage',
            'tournament_name',
            'match_type',
            'if_contain_india_team',
            'if_holiday',
            'match_time',
            'if_weekend',
            'tournament_type',
            'teams',
            'continents',
            'teams_tier',
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
    sub_wt_label: 1,
    reach_rate_label: 0.1
}