import configparser
import sys
import os
import re
import gc
import codecs
import shutil
import logging
import subprocess
import functools as ft
import pygrib
import warnings
import rasterio
import pandas as pd
import geopandas as gpd
import sqlalchemy.engine
import time
import constants as ct
from ftplib import FTP
from shapely.geometry import Point
from shapely.errors import ShapelyDeprecationWarning
from datetime import datetime
from typing import List, Type
from rasterstats import zonal_stats
from sqlalchemy import create_engine, text
from datetime import timedelta

warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

logger = logging.getLogger(__name__)


# Prerequisites
# Lease point layer having lease_id and rainfall threshold

def get_connection_string(config_db, db_name):
    connect_string = 'mysql+pymysql://{}:{}@{}:{}/{}'.format(
        config_db['DB_USER'],
        config_db['DB_PASS'],
        config_db['HOST'],
        config_db['PORT'],
        db_name)
    print(connect_string)
    return connect_string


def create_directory(directory: str, delete=False) -> str:
    exists = os.path.exists(directory)
    if exists:
        if delete:
            delete_files(directory)
    elif not exists:
        os.makedirs(directory)
    return directory



def delete_files(directory: str) -> None:
    files = os.listdir(directory)
    if len(files) > 0:
        for fname in os.listdir(directory):
            fpath = os.path.join(directory, fname)
            try:
                shutil.rmtree(fpath)
            except OSError:
                os.remove(fpath)


def cmd_subprocess(cmd: List[str]) -> None:
    """
    Runs CMD
    Args:
        cmd (List[str]): Commands in list format.
    """
    try:
        proc = subprocess.Popen(
            cmd,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE
        )
        out, err = proc.communicate()
        logger.info(codecs.decode(out, 'UTF-8'))
        rc = proc.returncode
    except Exception as e:
        logger.error(e)
    else:
        if rc:
            logger.error(codecs.decode(err, 'UTF-8'))
            logger.error('----- ERROR: PROCESS INCOMPLETE -----')
            sys.exit(1)


def regex_find(regex: str, text: str) -> List[str]:
    """
    Perform regex pattern search "findall".
    Args:
        regex (str): Regex pattern
        text (str): Text

    Returns (List[str}): List of matched strings when there are matches, otherwise returns None.

    """
    pattern = re.compile(regex)
    match = pattern.findall(text)
    if len(match) > 0:
        return match


def list_grbs_not_today(file_dir: str) -> List[str]:
    """
    Finds dated GRB and other than GRB files.
    Args:
        file_dir (srt): File directory
    """
    files = []
    for f in os.listdir(file_dir):
        if f.endswith('grb'):
            match = regex_find(ct.REG_PATTERN_TODAY, f)
            if match is None:  # GRB not today's data
                files.append(f)
        else:  # File not GRB
            files.append(f)
    return files


def delete_grbs(file_dir: str) -> None:
    """
    Deletes outdated GRB and other than GRB files.
    Args:
        file_dir (str): Directory for raw GRB files.
    """
    logger.info('----- Delete GRB files other than today\'s data. -----')
    files = list_grbs_not_today(file_dir)
    if len(files) > 0:
        for f in files:
            fpath = os.path.join(file_dir, f)
            os.remove(fpath)
            logger.info(f'File {f} deleted.')
    else:
        logger.info(f'No data to delete.')


def list_files(f_dir, ext):
    files = [os.path.join(f_dir, f) for f in os.listdir(f_dir) if f.endswith(ext)]
    return files


class PQPF():
    def __init__(self, state, db: str):
        """
        Args:
            state (str): State abbreviation
        """
        self.config = configparser.ConfigParser()
        self.config.read(ct.CONFIG_INI)
        self.state = state.upper()
        self.data_root = os.path.join(ct.PQPF_DATA_DIR, state.lower())
        print(self.config.sections)
        self.connect_str = get_connection_string(self.config[db], self.config[self.state]['DB_NAME'])
        # Data directories
        self.inputs_dir = os.path.join(self.data_root, 'inputs')
        self.outputs_dir = create_directory(os.path.join(self.data_root, 'outputs'))
        self.grb_raw_dir = create_directory(os.path.join(ct.PQPF_DATA_DIR, 'raw'))
        self.grb_subsets_dir = create_directory(os.path.join(self.data_root, 'intermediate/subsets'), delete=True)
        self.tiffs_dir = create_directory(os.path.join(self.data_root, 'intermediate/tiffs'), delete=True)

        # Lease shapefile info
        self.lease_shp = os.path.join(self.inputs_dir, self.config[self.state]['LEASE_SHP'])
        if self.state == 'NC':
            self.use_cols = [
                    self.config[self.state]['LEASE_SHP_COL_LEASE_ID'],
                    self.config[self.state]['LEASE_SHP_COL_CMU_NAME'],
                    self.config[self.state]['LEASE_SHP_COL_RAIN_IN'],
                    'geometry'
                ]
        elif self.state == 'SC':
            self.use_cols = [
                    self.config[self.state]['LEASE_SHP_COL_LEASE_ID'],
                    'geometry'
                ]

    def files_to_download(self) -> List[str]:
        """
        List today's PQPF GRB files.
        Returns:
            object: list of today's PQPF GRB files
        """
        logger.info('----- PQPF GRB files to download -----')
        files = []
        files_to_download = []
        today = datetime.today().strftime('%Y%m%d')
        # Get GRB names for today
        for hour in ct.HOURS:
            fname = f'{ct.GRB_PREFIX}_{today}12f0{hour}.grb'
            files.append(fname)
        # Check today's GRB files are already in directory
        if len(files) > 0:
            for f in files:
                if f not in os.listdir(self.grb_raw_dir):
                    files_to_download.append(f)
            if len(files_to_download) > 0:
                for f in files_to_download:
                    logger.info(f)
                return files_to_download
            else:
                logger.info('There is no data to download.')
        else:
            logger.error('ERROR: Having a problem listing PQPF GRB files.')
            sys.exit(1)

    def download_grbs(self, files: List[str]) -> None:
        """
        Download PQPF GRB files from FTP.
        Args:
            files (List[str]): List of GRB files
        """
        logger.info('----- Download PQPF GRBs from FTP -----')
        try:
            if files:
                ftp = FTP(ct.FTP_URL)
                ftp.login()
                ftp.encoding = 'utf-8'
                ftp.cwd(ct.FTP_CWD)
                for fname in files:
                    grb_path = os.path.join(self.grb_raw_dir, fname)
                    with open(grb_path, 'wb') as f:
                        ftp.retrbinary("RETR " + fname, f.write)
                    logger.info(f'{fname} downloaded.')
                ftp.quit()
            else:
                logger.info('Skip download')
        except Exception as e:
            logger.error('ERROR: GRB file download failed.')
            logger.error(e)
            sys.exit(1)

    def small_grb(self) -> None:
        """
        Crop GRB files in small area.
        """
        logger.info('----- Subset GRB file for small area -----')
        try:
            delete_files(self.grb_subsets_dir)
            for grb in [os.path.join(self.grb_raw_dir, f) for f in os.listdir(self.grb_raw_dir) if f.endswith('.grb')]:
                out_grb_path = os.path.join(self.grb_subsets_dir, f'sbs_{os.path.basename(grb)}')
                cmd = ['wgrib2', grb, '-small_grib', self.config[self.state]['LON_WE'],
                       self.config[self.state]['LAT_SN'], out_grb_path]
                logger.info(cmd)
                cmd_subprocess(cmd)
        except Exception as e:
            logger.error('ERROR: Subset GRIB file failed.')
            logger.error(e)
            sys.exit(1)


    def grb_to_tiff(self, thresholds: List[float]) -> None:
        """
        Transform GRB to Tiff for each threshold.
        Args:
            thresholds (List[float]): List of unique rainfall thresholds
        """
        logger.info('----- Transform GRB to Tiff for each threshold-----')
        try:
            # Delete previous outputs
            delete_files(self.tiffs_dir)

            for grb_fpath in [os.path.join(self.grb_subsets_dir, f) for f in os.listdir(self.grb_subsets_dir) if
                              f.endswith('grb')]:
                grbs = pygrib.open(grb_fpath)
                fname = grb_fpath.split('_')[-1].split('.')[0]

                for idx, grb in enumerate(grbs):
                    upper_limit = grb.scaledValueOfUpperLimit
                    inches = round((upper_limit / 1000) / 25.4, 1)

                    for threshold in thresholds:
                        if threshold == inches:
                            rainfall_str = str(threshold).replace('.', 'p')
                            tiff_path = os.path.join(self.tiffs_dir, f'{ct.TIFF_PREFIX}_{fname}_{rainfall_str}.tif')
                            cmd = ['gdal_translate', '-b', f'{idx + 1}', '-of', 'GTiff', grb_fpath, tiff_path]
                            cmd_subprocess(cmd)
        except Exception as e:
            logger.error('ERROR: GRB to TIFF transformation failed.')
            logger.error(e)
            sys.exit(1)

    def get_thresholds(self) -> List[float]:
        """
        Get unique rain_in values.

        Args
            lease_shp (str): The lease shp name
        Returns (List[float]): A list of unique rainfall thresholds
        """
        logger.info('----- Get unique rainfall thresholds -----')
        try:
            gdf = gpd.read_file(self.lease_shp)
            gdf = gdf[self.use_cols]
            thresholds = sorted(gdf.rain_in.unique())  # Returns list of class numpy.float64
            if len(thresholds) > 0:
                thresholds_num = [float(threshold) for threshold in thresholds]
                thresholds_str = ', '.join([str(threshold) for threshold in thresholds])
                logger.info(f'Thresholds: {thresholds_str}')
                return thresholds_num
            else:
                raise
        except Exception as e:
            logger.error(e)
            sys.exit(1)
        else:
            del gdf
            gc.collect()

    def ras_values_to_pts(self) -> Type:
        """
        Assign PQPF raster values to leases.

        Returns (gpd.GeoDataFrame): DataFrame containing each lease's lease_id, cmu_name, rain_in, pqpf_24h, pqpf_48h, pqpf_72h
            columns with values.
        """
        logger.info('----- Get PQPF raster value for leases -----')
        try:
            gdf = gpd.read_file(self.lease_shp)
            gdf = gdf[self.use_cols]

            coords = [(Point(i).x, Point(i).y) for i in gdf.geometry]
            tiffs = list_files(self.tiffs_dir, '.tif')

            for tiff in tiffs:
                logger.info(f'Process: {tiff}')
                src = rasterio.open(tiff)
                fname = os.path.basename(tiff)
                match = regex_find(ct.REG_PATTERN_GRB_HOURS, fname)
                if match and len(match) > 0:
                    hour = f'{match[0][2:]}h'
                    rainfall_str = fname.split('_')[-1].split('.')[0]
                    rainfall_in = float(rainfall_str.replace('p', '.'))
                    gdf.query(f'rain_in == {rainfall_in}')
                    gdf[f'pqpf_{hour}'] = [x[0] for x in src.sample(coords)]
                    logger.info(f'pqpf_{hour} added.')
                else:
                    logger.error('The process cannot extract hours from file name.')
                    sys.exit(1)
            return gdf

        except Exception as e:
            logger.error(e)
            sys.exit(1)
        else:
            del gdf
            gc.collect()

    def cmu_mean(self, df) -> str:
        """
        Calculate each CMU mean value for qppf_24h, pqpf_48h, and pqpf_72h and saves the data in CSV.
        Args:
            df (DataFrame):

        Returns (str): CMU probability CSV file path
        """
        logger.info('----- Calculate CMU mean values -----')
        try:
            rename_cols = {'pqpf_24h': 'prob_1d_perc', 'pqpf_48h': 'prob_2d_perc', 'pqpf_72h': 'prob_3d_perc'}
            # Delete previous outputs
            delete_files(self.outputs_dir)
            out_csv_path = os.path.join(self.outputs_dir, f'pqpf_cmu_{datetime.today().date()}.csv')
            group_cols = [self.config[self.state]['LEASE_SHP_COL_CMU_NAME']]
            metric_cols = ['pqpf_24h', 'pqpf_48h', 'pqpf_72h']
            aggs = df.groupby(group_cols)[metric_cols].mean() * 100
            aggs = aggs.round(0).astype(int)
            aggs.rename(columns=rename_cols, inplace=True)
            aggs.to_csv(out_csv_path)
            return out_csv_path

        except Exception as e:
            logger.error(e)
            sys.exit(1)
        else:
            del aggs
            gc.collect()

    def tiff_resample(self) -> None:
        """
        Resample TIFF raster file (approximately 25 meters).
        """
        resample_dir = os.path.join(self.data_root, 'intermediate/resample')
        create_directory(resample_dir, delete=True)
        tiffs = list_files(self.tiffs_dir, '.tif')
        for tiff in tiffs:
            out_fpath= os.path.join(resample_dir, os.path.basename(tiff))
            cmd = ['gdalwarp', '-tr', f'{ct.GRB_RES_X/100}', f'{ct.GRB_RES_Y/100}', '-r', 'bilinear', '-dstnodata',
                   '-999', '-overwrite', tiff, out_fpath]
            cmd_subprocess(cmd)

    def zonal_statis_to_df(self, shp, tiff) -> Type:
        """
        Args:
            shp (str): Polygon shapefile path
            tiff (str): TIFF raster file path

        Returns (gpd.GeoDataFrame):
        """
        stats = zonal_stats(shp, tiff, geojson_out=True, all_touched=True)
        df = gpd.GeoDataFrame.from_features(stats)
        df = df[[self.config[self.state]['LEASE_SHP_COL_LEASE_ID'], 'mean']]
        return df

    def zonal_stats_to_csv(self) -> str:
        """
        Save calculated zonal statistics to CSV file.
        Returns (str): Output CSV file path
        """
        resample_dir = os.path.join(self.data_root, 'intermediate/resample')
        tiffs = list_files(resample_dir, '.tif')
        columns = {'24': 'prob_1d_perc', '48': 'prob_2d_perc', '72': 'prob_3d_perc'}
        out_csv_path = os.path.join(self.outputs_dir, f'pqpf_cmu_{datetime.today().date()}.csv')
        lease_id_field = self.config[self.state]['LEASE_SHP_COL_LEASE_ID']
        rename_field = self.config[self.state]['LEASE_SHP_COL_CMU_NAME']
        dfs = []

        for tiff in tiffs:
            fname = os.path.basename(tiff).split('.')[0]
            match = regex_find(ct.REG_PATTERN_GRB_HOURS, fname)
            if match and len(match) > 0:
                hour = f'{match[0][2:]}' # e.g. ['f024]
                probs = columns[hour]
                df = self.zonal_statis_to_df(self.lease_shp, tiff)
                df['mean'] = df['mean'] * 100
                df['mean'] = df['mean'].round(0).astype(int)
                df = df.rename({'mean': probs}, axis=1)
                dfs.append(df)

        if len(dfs) > 0:
            df_merge = ft.reduce(lambda left, right: pd.merge(left, right, on=lease_id_field), dfs)
            df_merge = df_merge.rename(columns={lease_id_field: rename_field})
            print(df_merge)
            df_merge = df_merge.sort_values(by=[rename_field], ascending=True)
            df_merge.to_csv(out_csv_path, index=False)
            return out_csv_path

    def save_to_db(self, csv_path: str) -> None:
        """
        Saves the data to DB.
        Args:
            csv_path (str): CMU probabilities CSV file path
        """
        logger.info('----- Save to DB -----')
        try:
            engine = create_engine(self.connect_str)
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, index_col=False)
                print(df.head())
                if len(df.index) > 0:
                    with engine.connect() as conn:
                        conn.execute(text('CALL DeleteCmuProbsToday();'))
                        df.to_sql('cmu_probabilities', con=conn, if_exists='append', index=False)
                        queryset = conn.execute(text('CALL SelectCmuProbsToday();'))
                        if queryset.rowcount == len(df.index):
                            logger.info(f'{queryset.rowcount} rows added to DB.')

        except Exception as e:
            logger.error(e)
            sys.exit(1)

    def db_connection_test(self):
        try:
            engine = sqlalchemy.create_engine(self.connect_str)
            conn = engine.connect()
            tables = conn.execute(text('SHOW TABLES;'))
            print(tables.all())
            conn.close()
            engine.dispose()
        except Exception as e:
            logger.error(e)

    def nc_main(self) -> None:
        """
        Runs NC PQPF data extraction and save the results in database.
        """
        delete_grbs(self.grb_raw_dir)
        files = self.files_to_download()
        self.download_grbs(files)
        self.small_grb()
        thresholds = self.get_thresholds()
        self.grb_to_tiff(thresholds)
        df = self.ras_values_to_pts()
        csv_path = self.cmu_mean(df)
        self.save_to_db(csv_path)

    def sc_main(self) -> None:
        """
        Runs NC PQPF data extraction and save the results in database.
        """
        start = time.process_time()
        thresholds = [float(self.config[self.state]['THRESHOLD'])]
        delete_grbs(self.grb_raw_dir)
        files = self.files_to_download()
        self.download_grbs(files)
        self.small_grb()
        self.grb_to_tiff(thresholds)
        self.tiff_resample()
        csv_path = self.zonal_stats_to_csv()
        self.save_to_db(csv_path)
        stop = time.process_time()
        elapsed = timedelta(seconds=stop - start)
        logger.info(f'Start: {start}|End: {stop}')
        logger.info(f'Duration: {elapsed}')

