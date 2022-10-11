import os.path
import unittest
import configparser
import os
from analysis.settings import CONFIG_INI, ASSETS_DIR
from preps.preproc.shellcast_leases import create_leases_objects, save_leases_to_db
from utils.utils import get_connection_string

"""
SET SQL_SAFE_UPDATES = 0;
delete from leases;
SET SQL_SAFE_UPDATES = 1;
"""


class TestLeases(unittest.TestCase):

    def setUp(self):
        self.test_csv = os.path.join(os.path.dirname(__file__), 'data', 'test_insert_update.csv')
        print(self.test_csv)
        centroid_dir = os.path.join(ASSETS_DIR, 'nc', 'data', 'spatial', 'outputs', 'dmf_data', 'lease_centroid')
        centroid_csv_fname = 'lease_centroids_db_wgs84.csv'
        self.centroid_csv_path = os.path.join(centroid_dir, centroid_csv_fname)
        self.test_csv = os.path.join(centroid_dir, 'test_upsert.csv')

        config = configparser.ConfigParser()
        config.read(CONFIG_INI)
        self.connect_string = get_connection_string(config['docker.mysql'], config['NC']['DB_NAME'])

    def test_save_leases_to_db_initial(self):
        leases_data = create_leases_objects(self.centroid_csv_path)
        results = save_leases_to_db(self.connect_string, leases_data)
        print(results)



if __name__ == '__main__':
    unittest.main()






