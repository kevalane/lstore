import unittest
from lstore.query import Query
from lstore.table import Table

class QuerySpec(unittest.TestCase):

    def setUp(self):
        self.table = Table("test", 5, 0)
        self.query = Query(self.table)

    def test_init(self):
        self.setUp()
        self.assertEqual(self.query.table, self.table)

    def test_delete(self):
        pass

    def test_insert(self):
        pass

    def test_select(self):
        pass

    def test_update(self):
        pass