# -*- coding: utf-8 -*-
"""
# ---- script header ----
script name: ndfd_get_forecast_data_script.py
purpose of script: This script grabs latest National Digital Forecast Dataset (NDFD) data from the NC State Climate Office (SCO) TDS server, reformats it, and stores it in a local directory.
author: sheila saia
email: ssaia@ncsu.edu
date created: 20200427


# ---- notes ----
notes:
ndfd catalog website: https://tds.climate.ncsu.edu/thredds/catalog/nws/ndfd/catalog.html

help:
pydap help: https://pydap.readthedocs.io/en/latest/developer_data_model.html
thredds help (with python code): https://oceanobservatories.org/thredds-quick-start/#python
to see the nc sco catalog website: https://tds.climate.ncsu.edu/thredds/catalog/nws/ndfd/catalog.html


"""

# %% to do list

# TODO (wishlist) could try to run this using the xarray library


import pandas # for data mgmt
import datetime as dt # for datetime mgmt
from .functions import append_list_as_row, get_sco_ndfd_data, tidy_sco_ndfd_data # see functions.py file
import logging

logger = logging.getLogger(__name__)


def ndfd_sco_data_raw(tabular_output_path, ndfd_sco_server_url):
    """

    Args:
        tabular_output_path: "tabular/outputs/ndfd_sco_data/ndfd_sco_data_raw/"
        ndfd_sco_server_url: "https://tds.climate.ncsu.edu/thredds/dodsC/nws/ndfd/"
    """
    logger.info('ndfd_sco_data_raw')
    try:
        # hardcode current day at 7am UCT for now
        today = dt.date.today()
        today_str = today.strftime("%Y%m%d") + "07" # "2021013107"
        today_uct = pandas.to_datetime(dt.datetime.strptime(today_str, "%Y%m%d%H"))
        datetime_now_uct = today_uct.tz_localize(tz = "UCT")

        # round up to nearest hour in uct
        datetime_now_uct_td = dt.timedelta(hours = datetime_now_uct.hour, minutes = datetime_now_uct.minute, seconds=datetime_now_uct.second, microseconds = datetime_now_uct.microsecond)
        to_hour = dt.timedelta(hours=round(datetime_now_uct_td.total_seconds()/3600))
        datetime_now_round_uct = pandas.to_datetime((dt.datetime.combine(datetime_now_uct, dt.time(0)) + to_hour), format = "%Y-%m-%d %H:%M").tz_localize(tz = "UCT")

        # calc other bounds
        datetime_midnighttoday_uct = pandas.to_datetime((datetime_now_round_uct.strftime("%Y-%m-%d") + " 00:00")).tz_localize("UCT")
        datetime_noontoday_uct = pandas.to_datetime((datetime_now_round_uct.strftime("%Y-%m-%d") + " 12:00")).tz_localize("UCT")
        datetime_midnightnextd_uct = datetime_midnighttoday_uct + pandas.DateOffset(days = 1)

        # determine datetime string to use for query using bounds
        if (datetime_now_round_uct >= datetime_midnighttoday_uct) & (datetime_now_round_uct < datetime_noontoday_uct): # between midnight and noon
            temp_datetime_uct_str = datetime_midnighttoday_uct.strftime("%Y-%m-%d %H:%M")
        elif (datetime_now_round_uct >= datetime_noontoday_uct) & (datetime_now_round_uct < datetime_midnightnextd_uct): # between noon and midnight
            temp_datetime_uct_str = datetime_noontoday_uct.strftime("%Y-%m-%d %H:%M")

        # get data
        temp_data = get_sco_ndfd_data(base_server_url = ndfd_sco_server_url, datetime_uct_str = temp_datetime_uct_str)

        # only append data when it exists
        if (len(temp_data) > 0):
            # tidy qpf and pop12 data
            temp_qpf_data_pd, temp_qpf_datetime_ymdh_str = tidy_sco_ndfd_data(ndfd_data = temp_data, datetime_uct_str = temp_datetime_uct_str, ndfd_var = "qpf")
            temp_pop12_data_pd, temp_pop12_datetime_ymdh_str = tidy_sco_ndfd_data(ndfd_data = temp_data, datetime_uct_str = temp_datetime_uct_str, ndfd_var = "pop12")

            # check if desired times were available, only keep when we have both
            if ((len(temp_qpf_data_pd) > 0) and (len(temp_pop12_data_pd) > 0)):

                # define export path
                # temp_qpf_data_path = tabular_output_path + "qpf_" + temp_qpf_datetime_ymdh_str +  ".csv" # includes date in file name
                # temp_pop12_data_path = tabular_output_path + "pop12_" + temp_pop12_datetime_ymdh_str + ".csv" # includes date in file name
                temp_qpf_data_path = tabular_output_path + "qpf.csv"
                temp_pop12_data_path = tabular_output_path + "pop12.csv"


                # export results
                temp_qpf_data_pd.to_csv(temp_qpf_data_path, index = False)
                temp_pop12_data_pd.to_csv(temp_pop12_data_path, index = False)

                # keep track of available data
                # temp_data_available_pd = pandas.DataFrame({'datetime_uct_str':[temp_datetime_uct_str], 'status':["available"]})
                # data_available_pd = data_available_pd.append(temp_data_available_pd, ignore_index = True)
                temp_data_log = [temp_datetime_uct_str, "available"]

                # export data availability (i.e., append new row to data_log.csv)
                # data_availability_path = tabular_output_path + "data_available_" + temp_datetime_ymdh_str +  ".csv"
                # data_available_pd.to_csv(data_availability_path, index = False)
                data_log_path = tabular_output_path + "data_log.csv"
                append_list_as_row(data_log_path, temp_data_log)

                # print status
                # print("exported " + temp_datetime_uct_str + " data")
                logger.info("exported " + temp_datetime_uct_str + " data")

            else:
                # keep track of available data
                # temp_data_available_pd = pandas.DataFrame({'datetime_uct_str':[temp_datetime_uct_str], 'status':["not_available"]})
                # data_available_pd = data_available_pd.append(temp_data_available_pd, ignore_index = True)
                temp_data_log = [temp_datetime_uct_str, "not_available"]

                # export data availability (i.e., append new row to data_log.csv)
                # data_availability_path = tabular_output_path + "data_available_" + temp_datetime_ymdh_str +  ".csv"
                # data_available_pd.to_csv(data_availability_path, index = False)
                data_log_path = tabular_output_path + "data_log.csv"
                append_list_as_row(data_log_path, temp_data_log)

                # print status
                # print("appended " + temp_datetime_uct_str + " data")
                logger.info("appended " + temp_datetime_uct_str + " data")

        else:
            # keep track of available data
            # temp_data_available_pd = pandas.DataFrame({'datetime_uct_str':[temp_datetime_uct_str], 'status':["not_available"]})
            # data_available_pd = data_available_pd.append(temp_data_available_pd, ignore_index = True)
            temp_data_log = [temp_datetime_uct_str, "not_available"]

            # export data availability (i.e., append new row to data_log.csv)
            # data_availability_path = tabular_output_path + "data_available_" + temp_datetime_ymdh_str +  ".csv"
            # data_available_pd.to_csv(data_availability_path, index = False)
            data_log_path = tabular_output_path + "data_log.csv"
            append_list_as_row(data_log_path, temp_data_log)

            # print status
            # print("did not append " + temp_datetime_uct_str + " data")
            logger.info("did not append " + temp_datetime_uct_str + " data")

    except Exception:
        raise