import configparser
import logging
from analysis.settings import CONFIG_INI
from preps.preproc.shellcast_preps import ShellCastPreps, save_cmu_to_db
from utils.utils import get_connection_string

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info('##### Start ShellCast NC PreProcess #####')

    SWITCHES = ['run_all', 'analysis_only', 'db_only']
    # ! IMPORTANT: MODIFY SWITCH
    SWITCH = 'db_only'
    STATE_ABBREV = 'NC'
    STATE = 'North Carolina'
    DB = 'db.docker.mysql'

    # ----- Config -----
    config = configparser.ConfigParser()
    config.read(CONFIG_INI)

    # Constractor
    preps = ShellCastPreps(STATE, STATE_ABBREV)

    if SWITCH == 'run_all':
        # ----- ANALYSIS -----
        preps.run_r_dmf_tidy_state_bounds()
        preps.run_r_dmf_tidy_cmu_bounds()
        preps.run_r_dmf_tidy_sga_bounds()

        # ----- DATABASE -----
        cmus = preps.extract_cmus_from_geojson()
        connect_string = get_connection_string(config[DB], config[STATE_ABBREV]['DB_NAME'])
        save_cmu_to_db(connect_string, cmus)

    elif SWITCH == 'analysis_only':
        preps.run_r_dmf_tidy_state_bounds()
        preps.run_r_dmf_tidy_cmu_bounds()
        preps.run_r_dmf_tidy_sga_bounds()

    elif SWITCH == 'db_only':
        cmus = preps.extract_cmus_from_geojson()
        connect_string = get_connection_string(config[DB], config[STATE_ABBREV]['DB_NAME'])
        save_cmu_to_db(connect_string, cmus)

    logger.info('##### End #####')

