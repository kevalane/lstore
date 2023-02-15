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

    def test_insert_succes(self):
        self.assertTrue(self.query.insert(1, 2, 3, 4, 5))
        self.assertTrue(self.query.insert(2, 3, 4, 5, 6))
        self.assertTrue(self.query.insert(3, 4, 1341345, 6, 7134134))

    def test_insert_fail(self):
        # too many columns
        self.assertFalse(self.query.insert(4, 123, 456, 25, 0, 1))
        # too few columns
        self.assertFalse(self.query.insert(4, 234, 567))
        # non-integer column
        self.assertFalse(self.query.insert(4, 'Bob', 567, 25, 0))

    def test_delete(self):
        self.query.insert(1, 123, 456, 18, 1)
        self.assertTrue(self.query.delete(1))

    def test_delete_fail(self):
        self.assertFalse(self.query.delete(1))
        self.query.insert(1, 123, 456, 18, 1)
        self.assertFalse(self.query.delete(2))

    def test_select_success(self):
        self.query.insert(1, 123, 456, 18, 1)
        self.query.insert(2, 456, 789, 20, 0)
        self.assertEqual(self.query.select(1, 0, [1,1,1,1,1]), [[1, 123, 456, 18, 1]])
        self.assertEqual(len(self.query.select(1, 0, [1,1,1,1])), 1)
        self.assertEqual(self.query.select(1, 0, [1,0,1,0,1]), [[1, 456, 1]])
        self.assertEqual(self.query.select(1, 0, [1,0,1,0]), [[1, 456]])