"""
Project: ShellCast South Carolina
Date: November 2022 - 2023
"""

from shellcast_pqpf.pqpf import utils
from shellcast_pqpf.pqpf import setup_logging
from constants import ROOT_DIR, LOGGING_SC

STATE = 'SC'

utils.create_log_files(STATE)

setup_logging.setup_logger_yaml(LOGGING_SC)

from shellcast_pqpf.pqpf import pqpf
import logging

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info(f'{"="*50}')
    logger.info('\t\t\t  Start SC ShellCast Analysis')
    logger.info(f'{"="*50}')
    # DB connection information in config.ini
    db = 'gcp.mysql'

    # --- PQPF analysis ---
    pqpf = pqpf.PQPF(STATE, db)
    pqpf.sc_main()
    # ---------------------
    logger.info(f'{"=" * 50}')