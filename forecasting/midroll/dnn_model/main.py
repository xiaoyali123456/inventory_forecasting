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
    max_token = 100
    # if_mask_knock_off_matches = True
    filename = 'data/LiveMatch/data.csv'
    all_df = pd.read_csv(filename).rename(columns=lambda x: x.strip())
    label_split_list = [[0], [2], [1], [3]]
    # test_tournaments_list = [['wc2019'], ['wc2021'], ['ipl2022'], ['ac2022'], ['wc2022'], ['ac2023'], ['wc2023']]
    test_tournaments_list = [['wc2022']]
    for label_idx_list in label_split_list:
        for test_tournaments in test_tournaments_list:
            print(label_idx_list, test_tournaments)
            if_mask_knock_off_matches = True
            model = LiveMatchRegression(all_df, label_idx_list, test_tournaments, max_token, if_mask_knock_off_matches)
            model.train()
            model.test()
            if_mask_knock_off_matches = False
            model = LiveMatchRegression(all_df, label_idx_list, test_tournaments, max_token, if_mask_knock_off_matches)
            model.train()
            model.test()


if __name__ == '__main__':
    # flags.DEFINE_string('model', "live-reach", 'model project name')
    # app.run(main=main)
    main()

