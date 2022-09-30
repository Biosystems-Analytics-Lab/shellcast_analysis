import unittest
from ..utils import utils

class TestUtils(unittest.TestCase):

    def test_get_utc_datetime(self):
        result = utils.get_utc_datetime_now()
        print(result)



if __name__ == '__main__':
    unittest.main()