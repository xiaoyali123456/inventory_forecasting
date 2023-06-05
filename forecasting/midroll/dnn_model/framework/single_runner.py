
class SingleRunner(object):
    def __init__(self, model):
        self.model_class = model

    def run(self, _args):
        # label_split_list = [[0, 2], [1, 3]]
        label_split_list = [[0], [2], [1], [3]]
        test_tournaments_list = [['wc2021'], ['ipl2022'], ['ac2022'], ['wc2022'], ['ac2023'], ['wc2023']]
        for label_idx_list in label_split_list:
            for test_tournaments in test_tournaments_list:
                print(label_idx_list, test_tournaments)
                model = self.model_class(label_idx_list, test_tournaments)
                model.train()
                model.test()
                model.save(f'model_{"_and_".join([str(i) for i in label_idx_list])}_of{"_and_".join(test_tournaments)}.ckpt')
