import unittest

from airflow.models import DagBag
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType


class MyTestCase(unittest.TestCase):
    # Trop lourd à mettre en oeuvre, remplacé par unit_test_dag.py

    def test_listFiles(self):
        self.assertEqual(True, True)  # add assertion here


if __name__ == '__main__':
    unittest.main()
