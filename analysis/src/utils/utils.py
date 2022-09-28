import os
import logging
import logging.config
import yaml
from analysis.settings import LOGS_DIR, LOG_CONFIG_YAML
from datetime import datetime
import pytz


logger = logging.getLogger(__file__)

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

def create_directory(file_dir):
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)

class Logs:
  @staticmethod
  def setup_logger():
    """
    Set logging system for LANDIVIZ PreProcTool.
    @param str log_file_name: Log file name
    """
    logging.info('Set up logger.')
    # logs_dir = os.path.join(ROOT_DIR, 'logs')

    if not os.path.exists(LOGS_DIR):
      os.makedirs(LOGS_DIR)

    with open(LOG_CONFIG_YAML, 'r') as stream:
      config = yaml.load(stream, Loader=yaml.FullLoader)
    logging.config.dictConfig(config)
    logging.captureWarnings(True)

  @staticmethod
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





# class Settings:
#   def setup_logger(self, log_file_name):
#     """
#     Set logging system for LANDIVIZ PreProcTool.
#     @param str log_file_name: Log file name
#     """
#     logging.info('Set up logger.')
#     app_path = Config.ANALYSIS_PATH
#     logs_dir = os.path.join(app_path, 'logs')
#     logging_yaml = os.path.join(app_path, 'src', 'logging', 'logging.yaml')
#     logs_file = os.path.join(logs_dir, log_file_name)
#
#     if not os.path.exists(logs_dir):
#       os.makedirs(logs_dir)
#
#     with open(logging_yaml, 'r') as stream:
#       config = yaml.load(stream, Loader=yaml.FullLoader)
#     config['handlers']['file_handler']['filename'] = logs_file
#     logging.config.dictConfig(config)
#     logging.captureWarnings(True)
#
#   def error_log(self, logger, err):
#     """
#     Set error log format and write to a log file.
#     @param logger logger: Logger object
#     @param Exception err: Exception object
#     """
#     trace = []
#     tb = err.__traceback__
#     while tb is not None:
#       path = os.path.normpath(tb.tb_frame.f_code.co_filename)
#       path_last_two_level = '/'.join(path.split(os.sep)[-2:])
#       trace.append({
#         "filename": path_last_two_level,
#         "name": tb.tb_frame.f_code.co_name,
#         "line": tb.tb_lineno
#       })
#       tb = tb.tb_next
#     last_trace = trace[-1]
#     msg = f'{type(err).__name__}\t{last_trace["filename"]}:{last_trace["line"]}\n\t{str(err)}'
#     logger.error(msg)