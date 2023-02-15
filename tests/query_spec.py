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

    def test_delete_invalid_primary_key(self):
        self.query.insert(134134134, 123, 456, 18, 1)
        # mess up memory, corrupting indirection column
        self.table.base_pages[0].columns[0].put(31431741374613, 0)
        self.assertFalse(self.query.delete(134134134))

    def test_select_success(self):
        self.query.insert(1, 123, 456, 18, 1)
        self.query.insert(2, 456, 789, 20, 0)
        self.assertEqual(self.query.select(1, 0, [1,1,1,1,1])[0].columns, [1, 123, 456, 18, 1])
        self.assertEqual(len(self.query.select(1, 0, [1,1,1,1])), 1)
        self.assertEqual(self.query.select(1, 0, [1,0,1,0,1])[0].columns, [1, 456, 1])
        self.assertEqual(self.query.select(1, 0, [1,0,1,0])[0].columns, [1, 456])

    def test_select_fail(self):
        # no matching record
        self.assertFalse(self.query.select(3, 0, [1,1,1,1]))
        # too many projected columns
        self.assertFalse(self.query.select(1, 0, [1,1,1,1,1,1]))

    def test_update_success(self):
        self.query.insert(4444, 123, 456, 18, 1)
        self.assertTrue(self.query.update(4444, 1, 2, 3, 4, None))
        self.assertEqual(self.query.select(4444, 0, [1,1,1,1,1])[0].columns, [1, 2, 3, 4, 1])

    def test_update_failure(self):
        # record doesn't exist
        self.assertFalse(self.query.update(1, 1, 2, 19, 0, 1))
        # too many columns
        self.assertFalse(self.query.update(1, 1, 2, 19, 0, 1, 51, 15))
        # too few columns
        self.assertFalse(self.query.update(1, 1, 2, 19))        
        # non-integer column
        self.assertFalse(self.query.update(1, 1, 'Doe', 19, 0))

    def test_sum_success(self):
        self.query.insert(1, 123, 456, 18, 1)
        self.query.insert(2, 456, 789, 20, 0)
        self.assertEqual(self.query.sum(1, 3, 3), 38)

    def test_sum_failure(self):
        # no record in range
        self.assertFalse(self.query.sum(4, 6, 2))
        # invalid column index
        self.assertFalse(self.query.sum(1, 3, 5))

    def test_increment_success(self):
        self.query.insert(1, 2, 3, 4, 5)
        self.assertTrue(self.query.select(1, 0, [1, 1, 1, 1, 1]))
        r_before = self.query.select(1, 0, [1, 1, 1, 1, 1])[0]
        self.query.increment(1, 2)
        r = self.query.select(1, 0, [1, 1, 1, 1, 1])[0]
        self.assertEqual(r.columns[2], r_before.columns[2] + 1)

    ## MILESTONE 2
    def test_select_version(self):
        self.query.select_version(1, 0, [1, 1], -1)

    def test_sum_version(self):
        self.query.sum_version(1, 1, 1, -1)