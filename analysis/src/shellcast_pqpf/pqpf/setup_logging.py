import logging.config
import yaml


def setup_logger_yaml(logging_yaml_fpath):
    with open(logging_yaml_fpath, 'r') as stream:
        config = yaml.load(stream, Loader=yaml.FullLoader)

    logging.config.dictConfig(config)
    logging.captureWarnings(True)
