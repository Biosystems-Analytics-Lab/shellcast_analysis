import configparser
import os
import logging
from analysis.settings import CONFIG_INI, ASSETS_DIR, CENTROID_DIR, LEASE_CENTROIDS_CSV
from preps.preproc.shellcast_leases import ShellCastLeases, save_leases_to_db
from utils.utils import get_connection_string

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info('##### Start ShellCast NC Leases Analysis #####')
    # Read config.ini
    SWITCHES = ['run_all', 'analysis_only', 'db_only']
    # ! IMPORTANT: MODIFY SWITCH
    SWITCH = 'db_only'
    STATE_ABBREV = 'NC'
    DB = 'db.docker.mysql'

    # ----- Config -----
    config = configparser.ConfigParser()
    config.read(CONFIG_INI)
    connect_string = get_connection_string(config[DB], config[STATE_ABBREV]['DB_NAME'])

    # Constructor
    leases = ShellCastLeases(
        STATE_ABBREV, config[STATE_ABBREV]['LEASE_BOUNDARY_SHAPEFILE'], config[STATE_ABBREV], config[DB])

    if SWITCH == 'run_all':
        # Analysis
        lease_centroid_db_wgs84 = leases.run_r_dmf_tidy_lease_data()
        print(lease_centroid_db_wgs84)
        # Save to database
        save_leases_to_db(connect_string, lease_centroid_db_wgs84)

    elif SWITCH == 'analysis_only':
        lease_centroid_db_wgs84 = leases.run_r_dmf_tidy_lease_data()
        print(lease_centroid_db_wgs84)

    elif SWITCH == 'db_only':
            centroid_dir = os.path.join(ASSETS_DIR, f'{STATE_ABBREV.lower()}{CENTROID_DIR}')
            centroid_csv_fpath = os.path.join(centroid_dir, LEASE_CENTROIDS_CSV)

            # Save to database
            results = save_leases_to_db(connect_string, centroid_csv_fpath)
            print(results)

    logger.info('##### End #####')
