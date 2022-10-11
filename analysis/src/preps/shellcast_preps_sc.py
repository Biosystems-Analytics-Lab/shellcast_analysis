import configparser
from analysis.settings import CONFIG_INI
from preps.preproc.shellcast_preps import ShellCastPreps
from utils.utils import get_connection_string, save_cmu_to_db


if __name__ == '__main__':
    preps = ShellCastPreps('South Carolina', 'SC')
    # ----- ANALYSIS -----
    # preps.run_r_dmf_tidy_state_bounds()
    # preps.run_r_dmf_tidy_cmu_bounds()
    # preps.run_r_dmf_tidy_sga_bounds()

    # ----- DATABASE -----
    config = configparser.ConfigParser()
    config.read(CONFIG_INI)
    cmus = preps.extract_cmus_from_geojson()
    connect_string = get_connection_string(config['docker.mysql'], config['SC']['DB_NAME'])

    if len(cmus) > 0:
        save_cmu_to_db(connect_string, cmus)
