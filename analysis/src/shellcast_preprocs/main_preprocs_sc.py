import os
import configparser
import logging
from db_procs.db_utils import save_leases_to_db
from db_procs.utils import get_connection_string
from constants import PREPROC_DATA_DIR, CONFIG_INI
from db_procs.utils import setup_logger

setup_logger('sc_leases.log', logging.DEBUG)
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    print('ShellCast SC Preprocess')
    STATE_ABBREV = 'SC'
    DB = 'gcp.mysql'

    # ----- Config -----
    config = configparser.ConfigParser()
    config.read(CONFIG_INI)
    print(config.sections())
    connect_string = get_connection_string(config[DB], config[STATE_ABBREV]['DB_NAME'])

    centroid_dir = os.path.join(PREPROC_DATA_DIR, f'{STATE_ABBREV.lower()}')
    centroid_csv_fpath = os.path.join(centroid_dir, config[STATE_ABBREV]['LEASES_CENTROID'])

    # Save to database
    results = save_leases_to_db(connect_string, centroid_csv_fpath)
    print(results)
