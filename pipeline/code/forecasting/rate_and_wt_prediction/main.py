from rate_and_wt_prediction.dnn_regression import LiveMatchRegression
import pandas as pd
import sys


def main(DATE):
    # if_mask_knock_off_matches = True
    filename = '../../../../forecasting/midroll/dnn_model/data/LiveMatch/data.csv'
    all_df = pd.read_csv(filename).rename(columns=lambda x: x.strip())
    label_list = ['frees_watching_match_rate',
                  "watch_time_per_free_per_match",
                  'subscribers_watching_match_rate',
                  "watch_time_per_subscriber_per_match"]
    # test_tournaments_list = [['wc2019'], ['wc2021'], ['ipl2022'], ['ac2022'], ['wc2022'], ['ac2023'], ['wc2023']]
    test_tournaments_list = [['wc2022']]
    for label in label_list:
        for test_tournaments in test_tournaments_list:
            if_mask_knock_off_matches = True
            model = LiveMatchRegression(DATE, all_df, label, test_tournaments, max_token, if_mask_knock_off_matches)
            model.train()
            model.test()
            if_mask_knock_off_matches = False
            model = LiveMatchRegression(DATE, all_df, label, test_tournaments, max_token, if_mask_knock_off_matches)
            model.train()
            model.test()


if __name__ == '__main__':
    DATE = sys.argv[1]
    request = load_requests(DATE)
    if request != {}:
        main(DATE)

