import os

ROOT_DIR = os.path.dirname(__file__)
ASSETS_DIR = os.path.join(ROOT_DIR, 'assets')
SRC_DIR = os.path.join(ROOT_DIR, 'src')
LOGS_DIR = os.path.join(ROOT_DIR, 'logs')
LOG_CONFIG_YAML = os.path.join(ROOT_DIR, 'logging.yaml')
CONFIG_INI = os.path.join(ROOT_DIR, 'config.ini')
CENTROID_DIR = '/data/spatial/outputs/dmf_data/lease_centroids'
LEASE_CENTROIDS_CSV = 'lease_centroids_db_wgs84.csv'