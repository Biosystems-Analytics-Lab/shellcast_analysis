import os
import logging
import sys
import json
import configparser
from collections import Counter
import rpy2.robjects as robjects
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy import delete
from analysis.settings import ASSETS_DIR, SRC_DIR, CONFIG_INI
from analysis.src.utils.utils import Logs
import db_connect
from analysis.settings import ASSETS_DIR, SRC_DIR, LOGS_DIR, CONFIG_INI
from models import Cmu

if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

logging.basicConfig(
     filename=os.path.join(LOGS_DIR, 'preps.log'),
     level=logging.INFO,
     format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
     datefmt='%H:%M:%S'
 )

# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# set a format which is simpler for console use
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger('').addHandler(console)
logger = logging.getLogger(__name__)


class ShellCastPreps:

    def __init__(self, state_name, state_abbrev):
        self.log = Logs()
        self.state_name = state_name
        self.state_abbrev = state_abbrev

        # R script file directory
        self.r_scripts_dir = os.path.join(SRC_DIR, 'preps', 'r_scripts')

        # Spatial data directories
        spatial = f'{state_abbrev.lower()}/data/spatial'
        tabular = f'{state_abbrev.lower()}/data/tabular'
        self.spatial_idata_dir = os.path.join(ASSETS_DIR, spatial, 'inputs')
        self.spatial_odata_dir = os.path.join(ASSETS_DIR, spatial, 'outputs')

        # Tabular data directories
        self.tabular_idata_dir = os.path.join(ASSETS_DIR, tabular, 'inputs')
        self.tabular_odata_dir = os.path.join(ASSETS_DIR, tabular, 'outputs')


    def run_r_dmf_tidy_state_bounds(self):
        logger.info('Run dmf_tidy_state_bounds.R')
        try:
            r_script_fpath = os.path.join(self.r_scripts_dir, 'dmf_tidy_state_bounds.R')
            state_bounds_spatial_data_input_path = os.path.join(self.spatial_idata_dir, 'state_bounds_data/state_bounds_raw/')
            state_bounds_spatial_data_output_path = os.path.join(self.spatial_idata_dir, 'state_bounds_data/state_bounds/')
            state_bound_shp = 'State_Boundaries.shp'

            r = robjects.r
            r['source'](r_script_fpath)
            r_func = robjects.globalenv['dmf_tidy_state_bounds']

            r_func(state_bounds_spatial_data_input_path, state_bounds_spatial_data_output_path, self.state_name, self.state_abbrev, state_bound_shp)

        except Exception as e:
            self.log.error_log(logger, e)
            sys.exit()

    def run_r_dmf_tidy_cmu_bounds(self):
        """
        !IMPORTANT: Manual process required for
        * cmu_bounds_raw_valid_albers.shp
        * /tabular/inputs/dmf_rainfall_thresholds/rainfall_thresholds_raw_tidy.csv
        * /tabular/inputs/dmf_rainfall_thresholds/sga_key.csv
        * /tabular/inputs/dmf_rainfall_thresholds/cmu_sga_key.csv
        See developer's notes in R script.
        """
        logger.info('Run dmf_tidy_cmu_bounds.R')
        try:
            r_script_fpath = os.path.join(self.r_scripts_dir, 'dmf_tidy_cmu_bounds.R')
            cmu_spatial_data_input_path = os.path.join(self.spatial_idata_dir, 'dmf_data/cmu_bounds_raw/')
            rainfall_thresh_tabular_data_input_path = os.path.join(self.tabular_idata_dir, 'dmf_rainfall_thresholds/')
            cmu_spatial_data_output_path = os.path.join(self.spatial_idata_dir, 'dmf_data/cmu_bounds/')
            rainfall_thresh_tabular_data_output_path = os.path.join(self.tabular_idata_dir, 'dmf_rainfall_thresholds/')

            r = robjects.r
            r['source'](r_script_fpath)
            r_func = robjects.globalenv['dmf_tidy_cmu_bounds']

            r_func(cmu_spatial_data_input_path, rainfall_thresh_tabular_data_input_path, cmu_spatial_data_output_path, rainfall_thresh_tabular_data_output_path)
        except Exception as e:
            self.log.error_log(logger, e)
            sys.exit()

    def run_r_dmf_tidy_sga_bounds(self):
        """
        !IMPORTANT: Manual process required for
        * /spatial/inputs/dmf_data/sga_bounds_raw/sga_bounds_raw_valid_albers.shp
        """
        logger.info('Run dmf_tidy_sga_bounds.R')
        try:
            r_script_fpath = os.path.join(self.r_scripts_dir, 'dmf_tidy_sga_bounds.R')
            sga_spatial_data_input_path = os.path.join(self.spatial_idata_dir, 'dmf_data/sga_bounds_raw/')
            sga_spatial_data_output_path = os.path.join(self.spatial_idata_dir, 'dmf_data/sga_bounds/')

            r = robjects.r
            r['source'](r_script_fpath)
            r_func = robjects.globalenv['dmf_tidy_sga_bounds']

            r_func(sga_spatial_data_input_path, sga_spatial_data_output_path)

        except Exception as e:
            self.log.error_log(logger, e)
            sys.exit()

    def extract_cmus_from_geojson(self):
        cmu_geojson_fpath = os.path.join(self.spatial_idata_dir, 'dmf_data/cmu_bounds', 'cmu_bounds_wgs84.geojson')
        cmus = []

        with open(cmu_geojson_fpath) as f:
            data = json.load(f)

        if data:
            for d in data['features']:
                cmus.append(d['properties']['cmu_name'])
            cmus.sort()
            dups_cmus = [item for item, count in Counter(cmus).items() if count > 1]
            if len(dups_cmus) == 0:

                return cmus

            else:
                print('There are duplicates.')
                return []
        else:
            print('No data found in the file.')
            return []

def get_connection_string(config_db, db_name):
    connect_string = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(
        config_db['DB_USER'],
        config_db['DB_PASS'],
        config_db['HOST'],
        config_db['PORT'],
        db_name)
    print(connect_string)
    return connect_string


if __name__ == '__main__':
    preps = ShellCastPreps('North Carolina', 'NC')
    # preps.run_r_dmf_tidy_state_bounds()
    # preps.run_r_dmf_tidy_cmu_bounds()
    # preps.run_r_dmf_tidy_sga_bounds()

    config = configparser.ConfigParser()
    config.read(CONFIG_INI)
    cmus = preps.extract_cmus_from_geojson()

    if len(cmus):
        connect_string = get_connection_string(config['docker.mysql'], config['NC']['DB_NAME'])
        engine = create_engine(connect_string)
        session = Session(bind=engine)

        queryset = session.query(Cmu).all()
        if len(queryset) > 0:
            delete(Cmu)
            print(f'Delete {len(queryset)} rows.')
        cmus_obj = [Cmu(id=item, cmu_name=item) for item in cmus]
        print(cmus_obj)
        print(f'Save {len(cmus_obj)} rows.')
        session.bulk_save_objects(cmus_obj)
        print('----- Insert success -----')

        session.commit()
