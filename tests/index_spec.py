import unittest
from lstore.index import Index
from lstore.table import Table, Record
from lstore.config import *

class IndexTestCase(unittest.TestCase):
    def test_init(self):
        table = Table("test", 4, 0)
        index = table.index
        self.assertEquals(index.indices, {0: {}})

    def setup(self):
        key = 0
        table = Table("test", 4, key)
        self.assertFalse(table.index.create_index(key))
        self.assertEqual(table.index.indices, {key: {}})
        self.assertTrue(table.index.create_index(1))
        self.assertTrue(table.index.create_index(2))
        return table.index, table

    def insert_records(self, index, table):
        record = Record(0, [0, 1, 2], 0)
        record2 = Record(0, [0, 33, 2], 1)
        record3 = Record(0, [55, 33, 2], 22)
        index.push_record_to_index(record)
        index.push_record_to_index(record2)
        index.push_record_to_index(record3)
        self.assertEquals(index.indices[0][0], [0, 1])
        self.assertEquals(index.indices[0][55], [22])
        self.assertEquals(index.indices[1][33], [1, 22])
        self.assertEquals(index.indices[2][2], [0, 1, 22])
        return index, table

    def test_create_index(self):
        print("test_create_index")
        self.setup()

    def test_push_record_to_index(self):
        # setup
        index, table = self.setup()
        index, table = self.insert_records(index, table)
    
    def test_remove_record_from_index(self):
        # setup
        index, table = self.setup()
        index, table = self.insert_records(index, table)
        record = Record(0, [244, 24, 2], 44)
        index.push_record_to_index(record)
        self.assertEquals(index.indices[0][244], [44])
        self.assertEquals(index.indices[1][24], [44])
        self.assertEquals(index.indices[2][2], [0, 1, 22, 44])

        index.remove_record_from_index(record)
        self.assertEquals(index.indices[0][244], [])
        self.assertEquals(index.indices[1][24], [])
        self.assertEquals(index.indices[2][2], [0, 1, 22])

    def test_locate(self):
        # setup
        index, table = self.setup()
        index, table = self.insert_records(index, table)
        self.assertEquals(index.locate(0, 0), [0, 1])
        self.assertEquals(index.locate(0, 55), [22])
        self.assertEquals(index.locate(1, 33), [1, 22])
        self.assertEquals(index.locate(2, 2), [0, 1, 22])
        self.assertEquals(index.locate(2, 3), [])

        record = Record(0, [244, 24, 2, 5], 44)
        self.assertEquals(index.locate(3, 5), [])
        index.push_record_to_index(record)
        self.assertEquals(index.locate(3, 5), [44])

    def test_locate_range(self):
        # setup
        index, table = self.setup()
        index, table = self.insert_records(index, table)
        self.assertEquals(index.locate_range(0, 0, 0), [0, 1])
        self.assertEquals(index.locate_range(0, 55, 0), [0, 1, 22])
        self.assertEquals(index.locate_range(1, 33, 1), [0, 1, 22])
        self.assertEquals(index.locate_range(2, 33, 1), [1, 22])
        self.assertEquals(index.locate_range(1, 3, 2), [0, 1, 22])

        record = Record(0, [244, 24, 2, 5], 44)

        self.assertEquals(index.locate_range(1, 245, 0), [22])
        self.assertEquals(index.locate_range(5, 34, 1), [1, 22])
        index.push_record_to_index(record)
        self.assertEquals(index.locate_range(1, 245, 0), [22, 44])
        self.assertEquals(index.locate_range(5, 34, 1), [44, 1, 22])

    def test_drop_index(self):
        # setup
        index, table = self.setup()
        index, table = self.insert_records(index, table)
        self.assertTrue(index.drop_index(0))
        self.assertTrue(index.drop_index(1))
        self.assertTrue(index.drop_index(2))
        self.assertFalse(index.drop_index(0))
        self.assertFalse(index.drop_index(1))
        self.assertFalse(index.drop_index(2))
        self.assertEquals(index.indices, {})


    def test_remove_no_index(self):
        table = Table("test", 4, 0)
        record = Record(0, [0, 1, 2], 0)
        self.assertEqual(table.index.indices, {0: {}})
        table.index.remove_record_from_index(record)
        self.assertEqual(table.index.indices, {0: {}})

    def test_push_duplicate(self):
        # setup
        index, table = self.setup()
        index, table = self.insert_records(index, table)
        record = Record(0, [0, 1, 2], 12)
        index.push_record_to_index(record)
        self.assertEquals(index.indices[0][0], [0, 1, 12])
        self.assertEquals(index.indices[0][55], [22])
        self.assertEquals(index.indices[1][33], [1, 22])
        self.assertEquals(index.indices[2][2], [0, 1, 22, 12])

        # test push duplicate
        index.push_record_to_index(record)
        self.assertEquals(index.indices[0][0], [0, 1, 12])
        self.assertEquals(index.indices[0][55], [22])
        self.assertEquals(index.indices[1][33], [1, 22])
        self.assertEquals(index.indices[2][2], [0, 1, 22, 12])

    def test_update_index(self):
        # setup
        index, table = self.setup()
        index, table = self.insert_records(index, table)
        record = Record(0, [0, 1, 2], 12)
        index.push_record_to_index(record)
        self.assertEquals(index.indices[0][0], [0, 1, 12])
        self.assertEquals(index.indices[0][55], [22])
        self.assertEquals(index.indices[1][33], [1, 22])
        self.assertEquals(index.indices[2][2], [0, 1, 22, 12])

        # test update index
        record = Record(0, [0, 1, 2], 12)
        record2 = Record(0, [0, 1, 2], 12)
        record2.update_value(0, 55)
        record2.update_value(1, 33)
        record2.update_value(2, 2)
        index.update_index(record, record2)
        self.assertEquals(index.indices[0][0], [0, 1])
        self.assertEquals(index.indices[0][55], [22, 12])
        self.assertEquals(index.indices[1][33], [1, 22, 12])
        self.assertEquals(index.indices[2][2], [0, 1, 22, 12])

    def test_update_index(self):
        index, table = self.setup()

        record = Record(0, [1, 2, 3], 11)
        record2 = Record(0, [3, 0, 5], 22)
        record3 = Record(0, [1, 2, 5], 33)

        index.push_record_to_index(record)
        index.push_record_to_index(record2)
        index.push_record_to_index(record3)
        # what it should look like before
        self.assertEquals(index.indices[0][1], [11, 33])
        self.assertEquals(index.indices[1][2], [11, 33])
        self.assertEquals(index.indices[2][3], [11])

        index.update_index(record, record2, '101')

        # Post update
        self.assertEquals(index.indices[0][1], [33])
        self.assertEquals(index.indices[1][2], [33, 11])
        self.assertEquals(index.indices[2][3], [])

        

        
