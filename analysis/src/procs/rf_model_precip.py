# -*- coding: utf-8 -*-
"""
# ---- script header ----
script name: rf_model_precip.py
purpose of script: This script predicts the given precipitation in inches for the given input parameters and calculates the risk.
email: ethedla@ncsu.edu
date created: 20210609
date updated: 20220715 (mshukun@ncsu.edu)

# ---- notes ----
notes:

help:

"""
import pandas as pd
import numpy as np
import joblib
import os
import re
import sys
import logging
from analysis.src.utils.utils import Logs
# from config import Config, Settings


logger = logging.getLogger(__name__)

HOURS = ['24', '48', '72']
CSV_FNAME_PREFIX = 'ndfd_cmu_calcs'
FINAL_OUT_CSV_FNAME = 'ndfd_cmu_calcs_rf.csv'

class RfModelPrecip:
    def __init__(self, ndfd_data_dir, rf_models_dir):
        self.logs = Logs()
        self.ndfd_data_dir = ndfd_data_dir
        self.rf_models_dir = rf_models_dir

    def convert_precip_risk(self, df, field_name):
        """
        Calculate the risk factor for the precipitation predicted.
         > 0.9  Very High	5
         > 0.75	High	    4
         > 0.5	Moderate	3
         > 0.25	Low	        2
         < 0.25	Very Low    1
        Args:
            df: dataframe
        """
        df[field_name] = np.where(df[field_name] >= (0.9*df['rainfall_thresh_in']), 5,
                                np.where(df[field_name] >= (0.75*df['rainfall_thresh_in']), 4,
                                    np.where(df[field_name] >= (0.5*df['rainfall_thresh_in']), 3,
                                        np.where(df[field_name] >= (0.25*df['rainfall_thresh_in']), 2, 1))))


    def prediction(self, out_data_dir, in_csv_path, hours):
        """
        Perform analysis and outputs CSV file.
        Args:
            out_data_dir (str): Output file data directory.
            in_csv_path (str): Input CSV file full path (e.g. {path to}/ndfd_cmu_calcs{hours}.csv).
            hours (str): '24' or '48' or '72'

        Returns:
            str: Output CSV full path (e.g. {path to}/x24.csv)
        """
        try:
            columns = ['cmu_name', 'pop12_perc', 'qpf_in', 'rainfall_thresh_in', 'month']
            rf_model_path = os.path.join(self.rf_models_dir, f'joblib_RL{hours}_Model.pkl')
            out_csv_path = os.path.join(out_data_dir, f'x{hours}.csv')
            day = int(int(hours)/24)
            logger.info(f'\n----- Day {day} prediction -----')
            if os.path.exists(in_csv_path) and os.path.exists(out_data_dir):
                df = pd.read_csv(in_csv_path)
                dfx = df.loc[:, columns]
                dfx['cmu_num'] = dfx['cmu_name'].str[1:]
                dfx = dfx.drop(columns=['cmu_name'], axis=1)
                dfx = dfx.iloc[:, 0:5]

                joblib_hours_model = joblib.load(rf_model_path)
                # print('RF models loaded')
                logger.info('RF models loaded')
                y_pred = joblib_hours_model.predict(dfx)

                # Convert cmu_num to cmu_name
                dfx['cmu_name'] = dfx.apply(lambda row: 'U' + row.cmu_num, axis=1)
                dfx = dfx.drop(columns=['cmu_num'], axis=1)
                field_name = f'prob_{day}d_perc'
                dfx[field_name] = y_pred
                # print('Predicted the rainfall')
                logger.info('Predicted the rainfall')
                self.convert_precip_risk(dfx, field_name)
                # print('Converted to risk factor')
                logger.info('Converted to risk factor')

                dfx.to_csv(out_csv_path)
                logger.info(f'{os.path.basename(out_csv_path)} saved')
                return out_csv_path

        except Exception as e:
            self.logs.error_log(logger, e)
            sys.exit()


    def create_dataframe(self, csv_path):
        """
        Finds hours in file name using Regex and Creates DataFrame having columns ['cmu_name', prob_{day}d_perc']
        Args:
            csv_path (str): Input CSV file path

        Returns: DataFrame
        """
        fname = os.path.basename(csv_path)
        match_list = re.findall(r'\d+', fname)
        if len(match_list) > 0:
            if match_list[0] in HOURS:
                day = int(int(match_list[0])/24)
                df = pd.read_csv(csv_path, usecols=['cmu_name', f'prob_{day}d_perc'])
                return df


    def unique_dfs_row_count(self, dfs):
        """
        Creates a list of row counts in DataFrames and then finds unique value(s) in a list.
        Args:
            dfs (list): List of pandas.DataFrame objects.

        Returns:
            list: List of unique integer (DataFrame row counts). Successful result is one value in a list (e.g.[150]).
        """
        rows = []
        for df in dfs:
            rows.append(len(df.index))
        dups = list(set(rows))
        return dups


    def create_cmu_probability_csv(self, out_data_dir, out_csv_fname, csvs):
        """
        Merges prediction analysis CSV files (e.g. x24.csv, x48.csv, and x72.csv) and outputs merged CSV file.
        The merged CSV file contains 4 columns: ['cmu_name', 'prob_1d_perc', 'prob_2d_perc', 'prob_3d_perc'].
        Args:
            out_data_dir (str): Output file directory
            out_csv_fname (str): Example 'ndfd_cmu_calcs_rf.csv'
            csvs (list): Input CSV file full path list. For example:
                        [<full path to x24.csv>, <full path to x48.csv>, <full path to x72.csv>]
        """
        logger.info('Create CMU Probabilities CSV')
        try:
            if os.path.exists(out_data_dir):
                out_csv_path = os.path.join(out_data_dir, out_csv_fname)
                df1 = self.create_dataframe(csvs[0])
                df2 = self.create_dataframe(csvs[1])
                df3 = self.create_dataframe(csvs[2])
                dups = self.unique_dfs_row_count([df1, df2, df3])
                if len(dups) == 1:
                    cdf = pd.concat(
                        (idf.set_index('cmu_name') for idf in [df1, df2, df3]),
                        axis=1, join='outer'
                    )
                    if len(cdf.index) == dups[0]:
                        cdf.sort_index(key=lambda x: (x.to_series().str[1:].astype(int)), inplace=True)
                        cdf.to_csv(out_csv_path, index=True)
                        logger.info(f'{os.path.basename(out_csv_path)} saved.')
                    else:
                        raise Exception(f'Error: The cmu_names aren\'t consistent between x24.csv, x48.csv, and x72.csv files')
                else:
                    raise Exception('Error: Row count numbers don\'t match. Check row counts of files x24.csv, x48.csv, and x72.csv.')
        except Exception as e:
            self.logs.error_log(logger, e)
            sys.exit()

    def main(self):
        csvs_to_concat = []
        # Create prediction CSV files for 24, 48, 72 hours.
        for hour in HOURS:
            ndfd_csv_fname = f'{CSV_FNAME_PREFIX}_{hour}h.csv'
            in_csv_path = os.path.join(self.ndfd_data_dir, ndfd_csv_fname)
            csv_fpath = self.prediction(self.ndfd_data_dir, in_csv_path, hour)
            csvs_to_concat.append(csv_fpath)

        # Merge prediction CSV files
        self.create_cmu_probability_csv(self.ndfd_data_dir, FINAL_OUT_CSV_FNAME, csvs_to_concat)


