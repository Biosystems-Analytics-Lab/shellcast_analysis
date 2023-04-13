"""
Project: ShellCast North Carolina
Date: November 2022 - 2023
"""
import sys
import os

script_dir = os.path.join(os.path.dirname(__file__), 'src')
sys.path.append(script_dir)

import setup_logging

STATE = 'NC'

setup_logging.create_log_files(STATE)
setup_logging.setup_logger_yaml(os.path.join(os.path.dirname(__file__), 'logging_nc.yaml'))

from pqpf import pqpf
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
