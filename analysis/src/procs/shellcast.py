import os
import logging
import sys
import rpy2.robjects as robjects
from analysis.src.utils.utils import Logs
from analysis.settings import ASSETS_DIR, SRC_DIR
from analysis.src.procs.ndfd_get_forecast_data import ndfd_sco_data_raw
from analysis.src.procs.rf_model_precip import RfModelPrecip

logger = logging.getLogger(__file__)

class ShellCast:
    def __init__(self, state_abbrev, config):
        self.log = Logs()
        self.config = config

        # R script file directory
        self.r_scripts_dir = os.path.join(SRC_DIR, 'procs', 'r_scripts')

        # Spatial data directories
        spatial = f'{state_abbrev.lower()}/data/spatial'
        tabular = f'{state_abbrev.lower()}/data/tabular'
        self.spatial_idata_dir = os.path.join(ASSETS_DIR, spatial, 'inputs')
        self.spatial_odata_dir = os.path.join(ASSETS_DIR, spatial, 'outputs')

        # Tabular data directories
        self.tabular_idata_dir = os.path.join(ASSETS_DIR, tabular, 'inputs')
        self.tabular_odata_dir = os.path.join(ASSETS_DIR, tabular, 'outputs')

        # RF models directory
        self.rf_model_dir = os.path.join(ASSETS_DIR, state_abbrev.lower(), 'RF_models')

    def run_ndfd_get_forecast_data(self):
        """
        Runs procs.ndfd_get_forecast_data.ndfd_sco_data_raw
        """
        logger.info('Run ndfd_get_forecast_data.py')
        try:
            ndfd_sco_data_raw_dir = os.path.join(self.tabular_odata_dir, 'ndfd_sco_data/ndfd_sco_data_raw/')
            url = self.config['NDFD_SCO_SERVER_URL']
            ndfd_sco_data_raw(ndfd_sco_data_raw_dir, url)
            logger.info('------ Success ------')
        except Exception as e:
            self.log.error_log(logger, e)
            sys.exit()

    def run_r_convert_df_to_raster(self):
        logger.info('Run ndfd_convert_df_to_raster_script.R')
        try:
            r_script_fpath = os.path.join(self.r_scripts_dir, 'ndfd_convert_df_to_raster.R')
            ndfd_sco_data_raw_dir = os.path.join(self.tabular_odata_dir, 'ndfd_sco_data/ndfd_sco_data_raw/')
            bounds_dir = os.path.join(self.spatial_idata_dir, 'state_bounds_data/state_bounds/')
            out_data_dir = os.path.join(self.spatial_odata_dir, 'ndfd_sco_data/')
            bounds_10kmbuf_albers_shapefile = self.config['BOUNDS_10KMBUF_ALBERS_SHAPEFILE']

            r = robjects.r
            r['source'](r_script_fpath)
            r_func = robjects.globalenv['ndfd_convert_df_to_raster']

            r_func(ndfd_sco_data_raw_dir, bounds_dir, out_data_dir, bounds_10kmbuf_albers_shapefile)
            # r_script_path = os.path.join(self.forecast_nc_dir, "ndfd_convert_df_to_raster_script.R")
            # cmd = f'Rscript {r_script_path}'
            # self.r.run_r_script(cmd, out_fdir, out_csv_fname)
            logger.info('------ Success ------')
        except Exception as e:
            self.log.error_log(logger, e)
            sys.exit()

    def run_r_analyze_forecast(self):
        try:
            logger.info('Run ndfd_analyze_forecast_data.R')
            # Variables
            r_script_fpath = os.path.join(self.r_scripts_dir, 'ndfd_analyze_forecast_data.R')

            ndfd_sp_in_dir = os.path.join(self.spatial_odata_dir, 'ndfd_sco_data/')
            buffer_in_dir = os.path.join(self.spatial_idata_dir, 'dmf_data/sga_bounds/')
            cmu_bounds_in_dir = os.path.join(self.spatial_idata_dir, 'dmf_data/cmu_bounds/')
            lease_bounds_in_dir = os.path.join(self.spatial_odata_dir, 'dmf_data/lease_centroids/')
            rainfall_in_dir = os.path.join(self.tabular_idata_dir, 'dmf_rainfall_thresholds/')
            ndfd_sp_out_dir = os.path.join(self.spatial_odata_dir, 'ndfd_sco_data/')
            ndfd_tb_out_dir = os.path.join(self.tabular_odata_dir, 'ndfd_sco_data/')
            ndfd_tb_out_append_dir = os.path.join(self.tabular_odata_dir, 'ndfd_sco_data_appended/')

            r = robjects.r
            r['source'](r_script_fpath)
            r_func = robjects.globalenv['ndfd_analyze_forecast_data']

            r_func(ndfd_sp_in_dir, buffer_in_dir, cmu_bounds_in_dir, lease_bounds_in_dir, rainfall_in_dir, ndfd_sp_out_dir, ndfd_tb_out_dir, ndfd_tb_out_append_dir)
            # cmd = f'Rscript {r_script_path}'
            # run_r_script(cmd, out_fdir, out_csv_fname)

        except Exception as e:
            self.log.error_log(logger, e)
            sys.exit()

    def run_rf_model(self):
        try:
            ndfd_data_dir = os.path.join(self.tabular_odata_dir, 'ndfd_sco_data/cmu_calcs/')
            rf = RfModelPrecip(ndfd_data_dir, self.rf_model_dir)
            rf.main()
        except Exception as e:
            self.log.error_log(logger, e)
            sys.exit()

    def main(self):

        logger.info('##### ShellCast Analysis Started #####')
        try:
            # self.run_ndfd_get_forecast_data()
            # self.run_r_convert_df_to_raster()
            # self.run_r_analyze_forecast()
            self.run_rf_model()

            logger.info('##### ShellCast Analysis Ended #####')
        except Exception as e:
            print(e)