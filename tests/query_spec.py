import unittest
from lstore.query import Query
from lstore.table import Table
from lstore.config import *
import os

class QuerySpec(unittest.TestCase):

    def setUp(self):
        self.table = Table("test", 5, 0)
        self.query = Query(self.table)

    def test_init(self):
        self.setUp()
        self.assertEqual(self.query.table, self.table)

    def test_insert_success(self):
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
        base_page = self.table.bufferpool.retrieve_page(0, True, 5)
        base_page.columns[0].put(111, 0)
        self.table.bufferpool.touch_page(0, True)
        self.table.bufferpool.write_page(0, True)
        self.assertFalse(self.query.delete(134134134))

    def test_select_success(self):
        self.assertTrue(self.query.insert(1, 123, 456, 18, 1))
        self.assertTrue(self.query.insert(2, 456, 789, 20, 0))
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
        self.assertTrue(self.query.insert(4444, 123, 456, 18, 1))
        self.assertTrue(self.query.update(4444, 33, 2, 3, 4, None))
        self.assertEqual(self.query.select(4444, 0, [1,1,1,1,1])[0].columns, [33, 2, 3, 4, 1])

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

    def test_increment_no_record(self):
        self.assertFalse(self.query.increment(1134134, 0))

    ## MILESTONE 2
    def test_select_version(self):
        self.query.select_version(1, 0, [1, 1], -1)

    def test_sum_version(self):
        self.query.sum_version(1, 1, 1, -1)

    def test_update_duplicate_key(self):
        self.query.insert(1, 2, 3, 4, 5)
        self.query.insert(2, 3, 4, 5, 6)

        # change first records primary key to match second
        print(self.query.select(1, 0, [1, 1, 1, 1, 1]))
        self.assertFalse(self.query.update(1, 2, 33, 44, 55, 66))
        
    def test_add_duplicate_key(self):
        self.assertTrue(self.query.insert(1, 2, 3, 4, 5))
        self.assertFalse(self.query.insert(1, 2, 3, 4, 5))

    def test_select_non_primary_index(self):

        pass

    def test_select_no_index(self):
        self.assertTrue(self.query.insert(55, 2, 3, 4, 5))
        self.assertTrue(self.query.insert(66, 3, 4, 4, 6))
        self.assertTrue(self.query.insert(77, 4, 5, 4, 7))
        self.assertTrue(self.query.insert(88, 5, 6, 7, 8))
        self.assertEquals(len(self.query.select(5, 4, [1, 1, 1, 1, 1])), 1)
        self.assertEquals(self.query.select(5, 4, [1, 1, 1, 1, 1])[0].columns, [55, 2, 3, 4, 5])
        self.assertEquals(self.query.select(5, 4, [1, 0, 0, 0, 0])[0].columns, [55])

    def test_select_multiple_records(self):
        self.assertTrue(self.query.insert(55, 2, 3, 4, 5))
        self.assertTrue(self.query.insert(66, 3, 4, 4, 6))
        self.assertTrue(self.query.insert(77, 4, 5, 4, 7))
        self.assertTrue(self.query.insert(88, 5, 6, 7, 8))
        self.assertEquals(self.query.select(4, 3, [1, 1, 1, 1, 1])[0].columns, [55, 2, 3, 4, 5])
        self.assertEquals(self.query.select(4, 3, [1, 1, 1, 1, 1])[1].columns, [66, 3, 4, 4, 6])
        self.assertEquals(self.query.select(4, 3, [1, 1, 1, 1, 1])[2].columns, [77, 4, 5, 4, 7])
        self.assertEquals(len(self.query.select(4, 3, [1, 1, 1, 1, 1])), 3)
        self.assertEquals(self.query.select(4, 3, [0, 1, 0, 0, 0])[0].columns, [2])
        self.assertEquals(self.query.select(4, 3, [0, 1, 0, 0, 0])[1].columns, [3])
        self.assertEquals(self.query.select(4, 3, [0, 1, 0, 0, 0])[2].columns, [4])

    def test_select_no_record(self):
        self.assertTrue(self.query.insert(55, 2, 3, 4, 5))
        self.assertTrue(self.query.insert(66, 3, 4, 4, 6))
        self.assertTrue(self.query.insert(77, 4, 5, 4, 7))
        self.assertTrue(self.query.insert(88, 5, 6, 7, 8))
        result = self.query.select(1337, 2, [1, 1, 1, 1, 1])
        self.assertEquals(result, [])

    def test_update_no_record(self):
        self.assertTrue(self.query.insert(55, 2, 3, 4, 5))
        self.assertTrue(self.query.insert(66, 3, 4, 4, 6))
        self.assertTrue(self.query.insert(77, 4, 5, 4, 7))
        self.assertTrue(self.query.insert(88, 5, 6, 7, 8))
        self.assertFalse(self.query.update(1337, 2, 1337, 1337, 1337, 1337))

    def test_select_version(self):
        self.assertTrue(self.query.insert(14, 2, 3, 4, 5))
        self.assertTrue(self.query.update(14, None, 6, 7, 8, 9))
        self.assertTrue(self.query.update(14, None, 4, 11, 12, 13))

        # select the most recent version (version -1)
        result = self.query.select_version(14, 0, [1, 1, 1, 1, 1], -1)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].columns, [14, 6, 7, 8, 9])

        # select the oldest version (version -3)
        result = self.query.select_version(14, 0, [1, 1, 1, 1, 1], -3)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].columns, [14, 2, 3, 4, 5])

        # select a specific version (version -2)
        result = self.query.select_version(1, 0, [1, 1, 1, 1, 1], -2)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].columns, [14, 2, 3, 4, 5])

        # select version 0
        result = self.query.select_version(14, 0, [1, 1, 1, 1, 1], 0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].columns, [14, 4, 11, 12, 13])

    def test_select_version_fail(self):
        self.assertTrue(self.query.insert(14, 2, 3, 4, 5))
        self.assertTrue(self.query.update(14, None, 6, 7, 8, 9))
        self.assertTrue(self.query.update(14, None, 4, 11, 12, 13))

        # select a rid that doesn't exist
        self.assertFalse(self.query.select_version(67, 0, [1, 1, 1, 1, 1], -4))

        # pass too many projected columns
        self.assertFalse(self.query.select_version(14, 0, [1, 1, 1, 1, 1, 0, 0, 1], 1))


