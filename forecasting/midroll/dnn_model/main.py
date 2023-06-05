# This is a sample Python script.

from absl import app, flags, logging
from framework.single_runner import SingleRunner
from models.live_match_regression import LiveMatchRegression

runner_map = {
    'live-reach': (SingleRunner, LiveMatchRegression)
}


def main(argv):
    FLAGS = flags.FLAGS
    model_name = FLAGS.model
    runner_class, model_class = runner_map[model_name]
    runner = runner_class(model=model_class)
    runner.run(argv)


if __name__ == '__main__':
    flags.DEFINE_string('model', "live-reach", 'model project name')
    app.run(main=main)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
