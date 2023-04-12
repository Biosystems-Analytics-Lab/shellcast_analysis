"""
Project: ShellCast North Carolina
Date: November 2022 - 2023
"""

from shellcast_pqpf.pqpf import utils
from shellcast_pqpf.pqpf import setup_logging
from constants import LOGGING_NC

STATE = 'NC'

utils.create_log_files(STATE)

setup_logging.setup_logger_yaml(LOGGING_NC)

from shellcast_pqpf.pqpf import pqpf
import logging

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info(f'{"="*50}')
    logger.info('\t\t\t  Start NC ShellCast Analysis')
    logger.info(f'{"="*50}')
    # DB connection information in config.ini
    db = 'gcp.mysql'

    # --- PQPF analysis ---
    pqpf = pqpf.PQPF(STATE, db)
    pqpf.nc_main()
    # ---------------------
    logger.info(f'{"=" * 50}')
