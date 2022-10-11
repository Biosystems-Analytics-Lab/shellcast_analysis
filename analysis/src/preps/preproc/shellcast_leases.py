import time
import os
import sys
import pandas as pd
import rpy2.robjects as robjects
import logging
from analysis.settings import ASSETS_DIR, SRC_DIR, LOGS_DIR
from utils.utils import DataDoesNotExists
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from preps.preproc.models import Lease
from utils.utils import setup_logger, error_log

setup_logger('leases.log', logging.DEBUG)

logger = logging.getLogger(__name__)

robjects.r['options'](warn=-1)


class ShellCastLeases:
    def __init__(self, state_abbrev, lease_bounds_raw_shp, config, config_db):
        self.state_abbrev = state_abbrev
        self.lease_bounds_raw_shp = lease_bounds_raw_shp
        self.config = config
        self.config_db = config_db

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

        self.lease_centroid_db_wgs84 = os.path.join(self.spatial_odata_dir, 'dmf_data/lease_centroids',
                                                    'lease_centroids_db_wgs84.csv')

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

            r_func(lease_data_spatial_input_path, cmu_spatial_data_input_path, sga_spatial_data_input_path,
                   rainfall_thresh_tabular_data_input_path, lease_data_spatial_output_path, self.lease_bounds_raw_shp)
            return self.lease_centroid_db_wgs84

        except Exception as e:
            error_log(logger, e)
            sys.exit()


def create_leases_objects(lease_centroid_db_wgs84):
    leases = []
    try:
        if os.path.exists(lease_centroid_db_wgs84):
            df = pd.read_csv(lease_centroid_db_wgs84)
            df_leases = df.to_dict(orient='records')

            for row in df_leases:
                lease = Lease(
                    lease_id=row['lease_id'],
                    cmu_name=row['cmu_name'],
                    grow_area_name=row['grow_area_name'],
                    grow_area_desc=row['grow_area_desc'],
                    rainfall_thresh_in=row['rainfall_thresh_in'],
                    latitude=row['latitude'],
                    longitude=row['longitude']
                )
                leases.append(lease)
        return leases
    except Exception as e:
        error_log(logger, e)


def df_flatten(df):
    """
    Ensure the Lease DataFrame object is flattened.
    The pandas.DataFrame.to_dict() function contains nested values for Decimal columns (decimal.Decimal object) when
    database query results are converted into DataFrame.

    Note: Make sure DataFrame column order when you need to compair dictionary values since DataFrame is converted into
    List object to assign values to dictionary.

    Args:
        df (DataFrame):

    Returns:
        results (List[dict]):
    """
    results = []
    for row in df.values:
        data = dict(lease_id=row[0],
                    grow_area_name=row[1],
                    grow_area_desc=row[2],
                    cmu_name=row[3],
                    rainfall_thresh_in=float(str(row[4])),
                    latitude=float(str(row[5])),
                    longitude=float(str(row[6]))
                    )
        results.append(data)
    return results


def db_updates_compare(data_in_db_df, new_data_df):
    """
    Compair and finds update required data.
    For the evaluation purpose, output data is saved in CSV format. The CSV file is saved in LOG_DIR.
    Args:
        data_in_db (DataFrame): Data currently in Lease table
        new_data (DataFrame): Newly generated data having the same lease_id in the Lease table.
        Note: DataFrames must have shared "lease_id".

    Returns:
        updates (List[dict]): A List of data for update
    """
    updates = []
    data_in_db = df_flatten(data_in_db_df)
    new_data = df_flatten(new_data_df)
    now = time.strftime('%Y%m%d-%H%M%S')
    out_csv_fpath = os.path.join(LOGS_DIR, f'lease_updates_{now}.csv')

    for dbd in data_in_db:
        for newd in new_data:
            if dbd['lease_id'] == newd['lease_id']:
                # If data changed
                if dbd != newd:
                    updates.append(newd)
                    break
    df = pd.DataFrame(updates)
    df.to_csv(out_csv_fpath)
    return updates


def save_leases_to_db(connect_string, centroid_csv_fpath):
    """
    Perform DB upsert - Insert data when data doesn't exist and update data when data is changed.
    Args:
        connect_string (str):
        centroid_csv_fpath (str):

    """
    engine = create_engine(connect_string)
    session = Session(bind=engine)
    session.begin()
    try:
        reorder_columns = ['lease_id', 'grow_area_name', 'grow_area_desc', 'cmu_name', 'rainfall_thresh_in',
                           'latitude', 'longitude']
        leases_csv_df = pd.read_csv(centroid_csv_fpath)[reorder_columns]
        results = {'new_data_rows': len(leases_csv_df), 'inserted': 0, 'updated': 0}
        if len(leases_csv_df) > 0:
            logger.info(f'{len(leases_csv_df)} rows of raw data.')
            queryset = session.query(Lease).all()
            num_of_db_rows = len(queryset)
            logger.info(f'{num_of_db_rows} rows in database.')

            # Initial data load
            if num_of_db_rows == 0:
                leases_data = create_leases_objects(centroid_csv_fpath)
                session.bulk_save_objects(leases_data)
                inserted = session.query(Lease).all()
                session.commit()
                results['inserted'] = len(inserted)
                logger.info(f'{len(inserted)} rows inserted.')

            # Upsert
            else:
                # df = pd.DataFrame(queryset)
                queryset_values = []
                for q in queryset:
                    queryset_values.append(q.to_dict())
                df = pd.DataFrame.from_records(queryset_values)
                ids = df['lease_id']
                # Get data for INSERT
                data_not_in_db = leases_csv_df[
                    ~leases_csv_df['lease_id'].isin(ids)].reset_index(drop=True).to_dict(orient='records')
                num_of_insert_rows = len(data_not_in_db)
                if num_of_insert_rows > 0:
                    session.bulk_insert_mappings(Lease, data_not_in_db)
                    session.commit()
                    results['inserted'] = num_of_insert_rows
                    # logger.info(f'----- {num_of_insert_rows} rows inserted. -----')
                else:
                    logger.info('----- There is no data to insert. -----')

                # Get data for UPDATE
                data_in_db = df[df['lease_id'].isin(leases_csv_df['lease_id'])]
                updates = db_updates_compare(data_in_db, leases_csv_df)
                num_of_update_rows = len(updates)
                if num_of_update_rows > 0:
                    session.bulk_update_mappings(Lease, updates)
                    session.commit()
                    results['updated'] = num_of_update_rows
                    # logger.info(f'----- {num_of_update_rows} rows updated. -----')
                    logger.info([row['lease_id'] for row in updates])
                else:
                    logger.info('----- There is no data to update.----- ')
            logger.info(results)

            return results
        else:
            raise DataDoesNotExists
    except Exception as e:
        session.rollback()
        error_log(logger, e)
        pass
    else:
        session.close()
        engine.dispose()
