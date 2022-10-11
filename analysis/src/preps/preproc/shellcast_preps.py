import os
import logging
import sys
import json
from collections import Counter
import rpy2.robjects as robjects
from analysis.settings import ASSETS_DIR, SRC_DIR
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from preps.preproc.models import Cmu
from utils.utils import DataDoesNotExists, setup_logger, error_log


setup_logger('leases.log', logging.DEBUG)

logger = logging.getLogger(__name__)

robjects.r['options'](warn=-1)


class ShellCastPreps:

    def __init__(self, state_name, state_abbrev):
        self.state_name = state_name
        self.state_abbrev = state_abbrev

        # R script file directory
        self.r_scripts_dir = os.path.join(SRC_DIR, 'preps', '../r_scripts')

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
            state_bounds_spatial_data_input_path = os.path.join(self.spatial_idata_dir,
                                                                'state_bounds_data/state_bounds_raw/')
            state_bounds_spatial_data_output_path = os.path.join(self.spatial_idata_dir,
                                                                 'state_bounds_data/state_bounds/')
            state_bound_shp = 'State_Boundaries.shp'

            r = robjects.r
            r['source'](r_script_fpath)
            r_func = robjects.globalenv['dmf_tidy_state_bounds']

            r_func(state_bounds_spatial_data_input_path, state_bounds_spatial_data_output_path, self.state_name,
                   self.state_abbrev, state_bound_shp)

        except Exception as e:
            error_log(logger, e)
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

            r_func(cmu_spatial_data_input_path, rainfall_thresh_tabular_data_input_path, cmu_spatial_data_output_path,
                   rainfall_thresh_tabular_data_output_path)
        except Exception as e:
            error_log(logger, e)
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
            error_log(logger, e)
            sys.exit()

    def extract_cmus_from_geojson(self):
        try:
            cmu_geojson_fpath = os.path.join(self.spatial_idata_dir, 'dmf_data/cmu_bounds', 'cmu_bounds_wgs84.geojson')
            cmus = []

            with open(cmu_geojson_fpath) as f:
                data = json.load(f)

            if len(data):
                for d in data['features']:
                    cmus.append(d['properties']['cmu_name'])
                cmus.sort()
                dups_cmus = [item for item, count in Counter(cmus).items() if count > 1]

                if len(dups_cmus) == 0:
                    logger.info('A list of CMUs are created from JSON file.')
                    return cmus
                else:
                    logger.info('There are duplicates.')
                    return []
            else:
                raise DataDoesNotExists
        except Exception as e:
            error_log(logger, e)


def save_cmu_to_db(connect_string, cmus_data):
    engine = create_engine(connect_string)
    session = Session(bind=engine)
    session.begin()
    try:
        if len(cmus_data) > 0:
            queryset = session.query(Cmu).all()
            if len(queryset) > 0:
                number_rows_deleted = session.query(Cmu).delete()
                print(f'Delete {number_rows_deleted} rows.')
            cmus_obj = [Cmu(id=item, cmu_name=item) for item in cmus_data]
            print(cmus_obj)
            print(f'Save {len(cmus_obj)} rows.')
            session.bulk_save_objects(cmus_obj)
            session.commit()
            logger.info('----- Insert Success -----')
        else:
            raise DataDoesNotExists
    except Exception as e:
        session.rollback()
        error_log(logger, e)
    else:
        session.close()
        engine.dispose()
