import os
import sqlalchemy
import configparser
import sys
import pandas as pd
import rpy2.robjects as robjects
import logging
from analysis.settings import ASSETS_DIR, SRC_DIR, LOGS_DIR, CONFIG_INI

if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)

logging.basicConfig(
     filename=os.path.join(LOGS_DIR, 'leases.log'),
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


# IMPORTANT!! Below needs to be imported after log set up
from analysis.src.utils.utils import Logs
import db_connect

logger = logging.getLogger(__name__)

robjects.r['options'](warn=-1)

class ShellCastLeases:
    def __init__(self, state_abbrev, lease_bounds_raw_shp, config, config_db):
        self.logs = Logs()
        self.state_abbrev = state_abbrev
        self.lease_bounds_raw_shp = lease_bounds_raw_shp
        self.config = config
        self.config_db = config_db

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

        self.lease_centroid_db_wgs84 = os.path.join(self.spatial_odata_dir, 'dmf_data/lease_centroids', 'lease_centroids_db_wgs84.csv')

    def run_r_dmf_tidy_lease_data(self):
        logger.info('Run dmf_tidy-lease_data.R')
        try:
            r_script_fpath = os.path.join(self.r_scripts_dir, 'dmf_tidy_lease_data.R')
            lease_data_spatial_input_path = os.path.join(self.spatial_odata_dir, 'dmf_data/lease_bounds_raw/')
            cmu_spatial_data_input_path = os.path.join(self.spatial_idata_dir, 'dmf_data/cmu_bounds/')
            sga_spatial_data_input_path = os.path.join(self.spatial_idata_dir, 'dmf_data/sga_bounds/')
            rainfall_thresh_tabular_data_input_path = os.path.join(self.tabular_idata_dir, 'dmf_rainfall_thresholds/')
            lease_data_spatial_output_path = os.path.join(self.spatial_odata_dir, 'dmf_data/')

            r = robjects.r
            r['source'](r_script_fpath)
            r_func = robjects.globalenv['dmf_tidy_lease_data']

            r_func(lease_data_spatial_input_path, cmu_spatial_data_input_path, sga_spatial_data_input_path, rainfall_thresh_tabular_data_input_path, lease_data_spatial_output_path, self.lease_bounds_raw_shp)
            return self.lease_centroid_db_wgs84

        except Exception as e:
            self.logs.error_log(logger, e)
            sys.exit()

    # def connect_to_db(self):
    #     try:
    #         connect_string = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(
    #             self.config_db['DB_USER'],
    #             self.config_db['DB_PASS'],
    #             self.config_db['HOST'],
    #             self.config_db['PORT'],
    #             self.config['DB_NAME'])
    #         print(connect_string)
    #
    #         engine = sqlalchemy.create_engine(connect_string, echo=False)
    #         conn = engine.connect()
    #         return conn
    #     except Exception as e:
    #         logger.error('Database connection failed.')
    #         self.logs.error_log(logger, e)
    #         sys.exit()

    def update_leases_db(self, data, conn):
        for row in data:
            update_query = f'UPDATE leases SET grow_area_name={row["grow_area_name"]}, \
            grow_area_desc={row["grow_area_desc"]}, \
            cmu_name={row["cmu_name"]}, \
            rainfall_thresh_in={row["rainfall_thresh_in"]}, \
            latitude={row["latitude"]}, \
            longitude={row["longitude"]} \
            WHERE lease_id = row["lease_id"]'
            conn.execute(update_query)

    def add_leases(self, lease_centroid_db_wgs84, conn):
        logger.info('Add Leases to database.')
        trans = conn.begin()
        try:
            if os.path.exists(lease_centroid_db_wgs84):
                # Add only new data to db table
                queryset = conn.execute('SELECT lease_id FROM leases')
                leases_data = pd.read_csv(lease_centroid_db_wgs84)

                # If data already exists in database
                if queryset.rowcount > 0:
                    df = pd.DataFrame(queryset)
                    ids = df['lease_id']
                    # df[df.index.isin(df1.index)]
                    data_in_db = df[df['lease_id'].isin(leases_data['lease_id'])].to_dict()
                    data_not_in_db = leases_data[
                        ~leases_data['lease_id'].isin(ids)].reset_index(drop=True)

                    # If there are new data
                    if data_not_in_db.shape[0] > 0:
                        # query = make_lease_sql_query(data_not_in_db)
                        logger.info(f'{len(df["lease_id"])} rows will be added to database')
                        data_not_in_db.to_sql('leases', conn, if_exists='append', index=False)
                        # conn.execute(query)
                        trans.commit()
                        # print('added lease data to mysql db')
                        logger.info(f'New data insert: {data_not_in_db.shape[0]} rows')

                    else:
                        # print('there were no new leases to add to the mysql db')
                        logger.info('there were no new leases to add to the mysql db')

                    # If existing data needs to be updated
                    if data_in_db.shape[0] > 0:
                        self.update_leases_db(data_in_db, conn)
                        logger.info(f'Data update: {data_in_db.shape[0]}')
                    else:
                        logger.info('Data update: 0 rows')

                # Add data to empty database table
                else:
                    # Add all data to db table.
                    leases_data.to_sql('leases', conn, if_exists='append', index=False)
                    # query = make_lease_sql_query(lease_spatial_data)
                    # conn.execute(query)
                    trans.commit()
                    # print('the mysql db was empty so all leases were added')
                    logger.info('the mysql db was empty so all leases were added')
        except Exception as e:
            trans.rollback()
            self.logs.error_log(logger, e)
            sys.exit()


if __name__ == '__main__':
    # Read config.ini
    config = configparser.ConfigParser()
    config.read(CONFIG_INI)

    leases = ShellCastLeases('NC', 'NCDMF_Lease_20220310.shp', config['NC'], config['docker.mysql'])
    # lease_centroid_db_wgs84 = leases.run_r_dmf_tidy_lease_data()
    # print(lease_centroid_db_wgs84)
    lease_centroid_db_wgs84 = '/opt/project/analysis/assets/nc/data/spatial/outputs/dmf_data/lease_centroids/lease_centroids_db_wgs84.csv'

    if lease_centroid_db_wgs84:
        conn = db_connect.connect_to_db(config['docker.mysql'], config['NC']['DB_NAME'])
        leases.add_leases(lease_centroid_db_wgs84, conn)

    conn.close()

