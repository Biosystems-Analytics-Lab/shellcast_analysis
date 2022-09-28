# -*- coding: utf-8 -*-
"""
# ---- script header ----
script name: gcp_update_mysqldb_script.py
purpose of script: This script updates the ShellCast MySQL database. Specifically it updates
the sga_min_max table, the ncdmf_leases table, and the closure_probabilities table.
email: ssaia@ncsu.edu
date created: 20200716
date updated: 20220714 (mshukun@ncsu.edu)


# ---- notes ----
notes:

help:
pymysql help: https://github.com/PyMySQL/PyMySQL
pymysql docs: https://pymysql.readthedocs.io/en/latest/

"""
import os
import pandas as pd

import logging
import sys
from .functions import make_lease_sql_query
from analysis.src.utils.utils import Logs
# sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))
# from config import Config, Settings, DevConfig

logger = logging.getLogger(__name__)

# st = Settings()
logs = Logs()

# def update_leases_db(data, conn):
#     for row in data:
#         update_query = f'UPDATE leases SET grow_area_name={row["grow_area_name"]}, \
#         grow_area_desc={row["grow_area_desc"]}, \
#         cmu_name={row["cmu_name"]}, \
#         rainfall_thresh_in={row["rainfall_thresh_in"]}, \
#         latitude={row["latitude"]}, \
#         longitude={row["longitude"]} \
#         WHERE lease_id = row["lease_id"]'
#         conn.execute(update_query)


def add_cmu_probabilities(cmu_data_path, conn):
    logger.info('Add CMU Probabilities to database.')
    trans = conn.begin()
    try:
        if os.path.exists(cmu_data_path):
            df = pd.read_csv(cmu_data_path, index_col=False)
            df.to_sql('cmu_probabilities', conn, if_exists='append', index=False)
            query_data = conn.execute('CALL SelectSmuProbsToday')
            trans.commit()
            if len(query_data) == len(df.index):
            # print('added cmu data to mysql db')
                logger.info(f'{len(query_data)} rows added to DB.')
            else:
                Exception('CMU Probabilities DB insert failed.')
    except Exception as e:
        trans.rollback()
        logs.error_log(logger, e)
        sys.exit()


# def add_leases(lease_spatial_data_path, conn):
#     logger.info('Add Leases to database.')
#     trans = conn.begin()
#     try:
#         if os.path.exists(lease_spatial_data_path):
#             # Add only new data to db table
#             queryset = conn.execute('SELECT lease_id FROM leases')
#             lease_spatial_data = pd.read_csv(lease_spatial_data_path)
#
#             if len(queryset) > 0:
#                 df = pd.DataFrame(queryset)
#                 ids = df['lease_id']
#                 # df[df.index.isin(df1.index)]
#                 data_in_db = df[df['lease_id'].isin(lease_spatial_data['lease_id'])].to_dict()
#                 data_not_in_db = lease_spatial_data[
#                     ~lease_spatial_data['lease_id'].isin(ids)].reset_index(drop=True)
#
#                 if data_not_in_db.shape[0] > 0:
#                     # query = make_lease_sql_query(data_not_in_db)
#                     logger.info(f'{len(df["lease_id"])} rows will be appended to database')
#                     data_not_in_db.to_sql('leases', conn, if_exists='append', index=False)
#                     # conn.execute(query)
#                     trans.commit()
#                     # print('added lease data to mysql db')
#                     logger.info(f'New data insert: {data_not_in_db.shape[0]} rows')
#
#                 else:
#                     # print('there were no new leases to add to the mysql db')
#                     logger.info('there were no new leases to add to the mysql db')
#
#                 if data_in_db.shape[0] > 0:
#                     update_leases_db(data_in_db, conn)
#                     logger.info(f'Data update: {data_in_db.shape[0]}')
#                 else:
#                     logger.info('Data update: 0 rows')
#
#             else:
#                 # Add all data to db table.
#                 lease_spatial_data.to_sql('leases', conn, if_exists='append', index=False)
#                 # query = make_lease_sql_query(lease_spatial_data)
#                 # conn.execute(query)
#                 trans.commit()
#                 # print('the mysql db was empty so all leases were added')
#                 logger.info('the mysql db was empty so all leases were added')
#     except Exception as e:
#         trans.rollback()
#         logs.error_log(logger, e)
#         sys.exit()



# if __name__ == '__main__':
#     DEV = True
#     print(Config.ANALYSIS_PATH)
#     cmu_data_path = os.path.join(Config.ANALYSIS_PATH, Config.CMU_DATA)
#     lease_spatial_data_path = os.path.join(Config.ANALYSIS_PATH, Config.LEASE_SPATIAL_DATA)
#
#
#
#
#     # add_cmu_probabilities(cmu_data_path, engine)
#     add_leases(lease_spatial_data_path, conn)

