import unittest
import os
import configparser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
from analysis import Base, Leases
from analysis.settings import ROOT, CONFIG_INI


config = configparser.ConfigParser()
config.read(os.path.join(CONFIG_INI))

class TestLeasesDB(unittest.TestCase):
    leases_csv_path = os.path.join(ROOT, config['forecast.nc']['LEASES_CSV'])
    connect_string = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4'.format(
        'mshukun', 'ShellCast!tm3', '127.0.0.1', '3306', 'shellcast_nc')
    engine = create_engine(connect_string)
    Session = sessionmaker(bind=engine)
    session = Session()

    def setUp(self):
        # Base.metadata.create_all(self.engine)
        df = pd.read_csv(self.leases_csv_path, index_col=False)
        df = df.rename(columns={'ncdmf_lease_id': 'lease_id', 'cmu_name': 'cmu_name'})
        self.session.bulk_insert_mappings(Leases, df.to_dict('records'))
        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def test_query(self):
        result = self.session.query(Leases).all()
        print(result)









if __name__ == '__main__':
    unittest.main()