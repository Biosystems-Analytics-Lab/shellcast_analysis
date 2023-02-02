
import sys
print(sys.getdefaultencoding())
import logging
from pqpf import pqpf


if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)
    # DB connection
    db = 'docker.mysql'

    # NC
    pqpf = pqpf.PQPF('NC', db)
    pqpf.nc_main()
    del pqpf