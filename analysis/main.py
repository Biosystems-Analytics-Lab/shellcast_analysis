import configparser
import os
import yaml
import logging.config
import sqlalchemy
import sys
from settings import CONFIG_INI, LOGS_DIR, LOG_CONFIG_YAML

if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

with open(LOG_CONFIG_YAML, 'r') as stream:
    log_config = yaml.load(stream, Loader=yaml.FullLoader)
logging.config.dictConfig(log_config)
logging.captureWarnings(True)

# IMPORTANT!! Below needs to be imported after log set up
from src.utils.utils import Logs
from src.procs.shellcast import ShellCast
from src.procs.gcp_update_mysqldb import add_cmu_probabilities

logger = logging.getLogger(__file__)

logs = Logs()


def connect_to_db(config_db, db_name):
    try:
        connect_string = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(
            config_db['DB_USER'],
            config_db['DB_PASS'],
            config_db['HOST'],
            config_db['PORT'],
            db_name)
        engine = sqlalchemy.create_engine(connect_string)
        conn = engine.connect()
        return conn
    except Exception as e:
        logger.error('Database connection failed.')
        logs.error_log(logger, e)
        sys.exit()

def analysis_to_db(state, config, db_section):
    state_upper = state.upper()
    shellcast = ShellCast(state_upper, config[state_upper])
    shellcast.main()
    connect_to_db(config[db_section], config[state_upper]['DB_NAME'])
    add_cmu_probabilities(state)


if __name__ == '__main__':
    # Read config.ini
    config = configparser.ConfigParser()
    config.read(CONFIG_INI)


    # RUN ANALYSIS AND SAVE TO DB
    # NC
    analysis_to_db('NC', config, 'localhost')
