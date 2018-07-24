from model.ner_model import NERModel
from model.config import Config
from proto.data_dict_pb2 import Char2IdDict, LabelId2ClassDict


def main():
    # create instance of config
    config = Config()

    # build model
    model = NERModel(config)
    model.build()
    model.restore_session(config.dir_model)

    char2id_dict = Char2IdDict()
    for ch, ch_id in config.vocab_words.items():
        char2id_dict.charMap[ch] = ch_id
    with open('results/output/char2id_dict', 'wb') as f:
        f.write(char2id_dict.SerializeToString())

    labelid2class_dict = LabelId2ClassDict()
    for ch, ch_id in config.vocab_tags.items():
        labelid2class_dict.labelIdMap[ch_id] = ch
    with open('results/output/labelid2class_dict', 'wb') as f:
        f.write(labelid2class_dict.SerializeToString())


if __name__ == "__main__":
    main()
