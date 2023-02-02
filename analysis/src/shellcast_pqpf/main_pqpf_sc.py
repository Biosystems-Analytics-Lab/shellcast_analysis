
"""
Project: ShellCast South Carolina
Date: November 2022 - 2023
Developer: Makiko Shukunobe
Organization: NCSU Center for Geospatial Analytics
Email: mshukun@ncsu.edu
"""
import logging
from pqpf import pqpf

if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    # DB connection information in config.ini
    db = 'gcp.local'

    # --- PQPF analysis ---
    pqpf = pqpf.PQPF('SC', db)
    # pqpf.db_connection_test()
    pqpf.sc_main()
    # ---------------------


