# This is a sample Python script.

# from absl import app, flags, logging
from framework.single_runner import SingleRunner
from models.live_match_regression import LiveMatchRegression
import pandas as pd

runner_map = {
    'live-reach': (SingleRunner, LiveMatchRegression)
}


def main():
    # FLAGS = flags.FLAGS
    # model_name = FLAGS.model
    # runner_class, model_class = runner_map[model_name]
    # runner = runner_class(model=model_class)
    # runner.run(argv)
    # filename = 'data/LiveMatch/data.csv'
    # all_df = pd.read_csv(filename).rename(columns=lambda x: x.strip())
    filename = 'data/LiveMatch/dataset.snappy.parquet'
    all_df = pd.read_parquet(filename).sort_values(['date', 'content_id'])
    all_df = pd.read_parquet(f"s3://adtech-ml-perf-ads-us-east-1-prod-v1/data/live_ads_inventory_forecasting/pipeline/all_features_hots_format_full_avod_and_simple_one_hot/")
    # print(all_df)
    label_split_list = [[0], [2], [1], [3]]
    # label_split_list = [[1]]
    # test_tournaments_list = [['wc2019'], ['wc2021'], ['ipl2022'], ['ac2022'], ['wc2022'], ['ac2023'], ['wc2023']]
    test_tournaments_list = [['wc2023']]
    for label_idx_list in label_split_list:
        for test_tournaments in test_tournaments_list:
            print(label_idx_list, test_tournaments)
            if_mask_knock_off_matches = False
            # for bs in [8, 16, 32, 64]:
            #     print(f"bs={bs}")
            #     for epoch in range(10, 110, 10):
            #         print(f"epoch={epoch}")
            #         for max_token in [32, 100]:
            #             for lr in [1e-2, 1e-3, 5e-3]:
            #                 for weight_decay in [1e-3, 1e-2, 0]:
            #                     model = LiveMatchRegression(all_df, label_idx_list, test_tournaments, max_token, if_mask_knock_off_matches,
            #                                                 num_epochs=epoch, batch_size=bs, lr=lr, weight_decay=weight_decay)
            #                     model.train()
            #                     model.test()
            model = LiveMatchRegression(all_df, label_idx_list, test_tournaments, if_mask_knock_off_matches)
            model.train()
            model.test()
            # if_mask_knock_off_matches = True
            # model = LiveMatchRegression(all_df, label_idx_list, test_tournaments, max_token, if_mask_knock_off_matches)
            # model.train()
            # model.test()


if __name__ == '__main__':
    # flags.DEFINE_string('model', "live-reach", 'model project name')
    # app.run(main=main)
    main()

