import os
import logging
import logging.config
import yaml
from analysis.settings import LOGS_DIR, LOG_CONFIG_YAML
from datetime import datetime
import pytz

logger = logging.getLogger(__file__)


class Error(Exception):
    pass


class DataDoesNotExists(Error):
    pass


def make_tz_aware_now(dt, tz='UTC', is_dst=None):
    """
    Add timezone information to a datetime object, only if it is naive.
    https://stackoverflow.com/questions/7065164/how-to-make-a-timezone-aware-datetime-object
    """
    tz = dt.tzinfo or tz
    try:
        tz = pytz.timezone(tz)
    except AttributeError:
        pass
    return tz.localize(dt, is_dst=is_dst)


def format_datetime(dt, format):
    dt_str = dt.strftime(format)
    return datetime.strptime(dt_str, format)


def get_utc_datetime_now():
    now = datetime.now()
    formatter = '%Y-%m-%d %H:%M:%S'
    now_str = now.strftime(formatter)
    now_micro_rounded = datetime.strptime(now_str, formatter)
    dt_utc = make_tz_aware_now(now_micro_rounded)
    return dt_utc


def get_connection_string(config_db, db_name):
    connect_string = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(
        config_db['DB_USER'],
        config_db['DB_PASS'],
        config_db['HOST'],
        config_db['PORT'],
        db_name)
    print(connect_string)
    return connect_string


def create_directory(file_dir):
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)


def setup_logger_yaml():
    logging.info('Set up logger.')
    # logs_dir = os.path.join(ROOT_DIR, 'logs')

    if not os.path.exists(LOGS_DIR):
        os.makedirs(LOGS_DIR)

    with open(LOG_CONFIG_YAML, 'r') as stream:
        config = yaml.load(stream, Loader=yaml.FullLoader)
    logging.config.dictConfig(config)
    logging.captureWarnings(True)


def setup_logger(log_fname, logging_level):
    # ----- FILE LOGGING SETTINGS -----
    if not os.path.exists(LOGS_DIR):
        os.makedirs(LOGS_DIR)

    logging.basicConfig(
        filename=os.path.join(LOGS_DIR, log_fname),
        level=logging_level,
        format='[%(asctime)s] {%(filename)s:%(lineno)s %(levelname)s:%(message)s}',
        datefmt='%m/%d/%Y %I:%M:%S %p'
    )

    # ----- STREAM LOGGING SETTINGS -----
    # set up logging to console
    console = logging.StreamHandler()
    console.setLevel(logging_level)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(filename)s:%(lineno)s %(levelname)s:%(message)s')
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)


def error_log(logger, err):
    """
Set error log format and write to a log file.
@param logger logger: Logger object
@param Exception err: Exception object
"""
    trace = []
    tb = err.__traceback__
    while tb is not None:
        path = os.path.normpath(tb.tb_frame.f_code.co_filename)
        path_last_two_level = '/'.join(path.split(os.sep)[-2:])
        trace.append({
            "filename": path_last_two_level,
            "name": tb.tb_frame.f_code.co_name,
            "line": tb.tb_lineno
        })
        tb = tb.tb_next
    last_trace = trace[-1]
    msg = f'{type(err).__name__}\t{last_trace["filename"]}:{last_trace["line"]}\n\t{str(err)}'
    logger.error(msg)
