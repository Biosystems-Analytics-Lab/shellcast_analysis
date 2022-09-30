import sqlalchemy
import logging
from analysis.src.utils.utils import Logs

logs = Logs()

logger = logging.getLogger(__name__)

def connect_to_db(config_db, db_name):
    try:
        connect_string = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(
            config_db['DB_USER'],
            config_db['DB_PASS'],
            config_db['HOST'],
            config_db['PORT'],
            db_name)
        print(connect_string)

        engine = sqlalchemy.create_engine(connect_string, echo=False)
        conn = engine.connect()
        return conn
    except Exception as e:
        logger.error('Database connection failed.')
        logs.error_log(logger, e)
