# This is a sample Python script.

# from absl import app, flags, logging
from framework.single_runner import SingleRunner
from models.live_match_regression import LiveMatchRegression

runner_map = {
    'live-reach': (SingleRunner, LiveMatchRegression)
}


def main():
    # FLAGS = flags.FLAGS
    # model_name = FLAGS.model
    # runner_class, model_class = runner_map[model_name]
    # runner = runner_class(model=model_class)
    # runner.run(argv)
    label_split_list = [[0], [2], [1], [3]]
    test_tournaments_list = [['wc2019'], ['wc2021'], ['ipl2022'], ['ac2022'], ['wc2022'], ['ac2023'], ['wc2023']]
    for label_idx_list in label_split_list:
        for test_tournaments in test_tournaments_list:
            print(label_idx_list, test_tournaments)
            model = LiveMatchRegression(label_idx_list, test_tournaments)
            model.train()
            model.test()


if __name__ == '__main__':
    # flags.DEFINE_string('model', "live-reach", 'model project name')
    # app.run(main=main)
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
