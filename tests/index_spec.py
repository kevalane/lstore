import unittest
from lstore.index import Index
from lstore.table import Table, Record

class IndexTestCase(unittest.TestCase):
    def test_init(self):
        table = Table("test", 3, 0)
        index = Index(table)
        self.assertEquals(index.indices, {})

    def setup(self):
        table = Table("test", 3, 0)
        index = Index(table)
        col = 0
        self.assertTrue(index.create_index(col))
        self.assertFalse(index.create_index(col))
        self.assertEquals(index.indices[col], {})
        return index, table

    def insert_records(self, index, table):
        record = Record(0, 0, [0, 1, 2])
        record2 = Record(1, 0, [0, 33, 2])
        record3 = Record(22, 0, [55, 33, 2])
        index.push_record_to_index(record)
        index.push_record_to_index(record2)
        index.push_record_to_index(record3)
        self.assertEquals(index.indices[0][0], [0, 1])
        self.assertEquals(index.indices[0][55], [22])
        self.assertEquals(index.indices[1][33], [1, 22])
        self.assertEquals(index.indices[2][2], [0, 1, 22])
        return index, table

    def test_create_index(self):
        self.setup()

    def test_push_record_to_index(self):
        # setup
        index, table = self.setup()
        index, table = self.insert_records(index, table)
    
    def test_remove_record_from_index(self):
        # setup
        index, table = self.setup()
        index, table = self.insert_records(index, table)
        record = Record(44, 0, [244, 24, 2])
        index.push_record_to_index(record)
        self.assertEquals(index.indices[0][244], [44])
        self.assertEquals(index.indices[1][24], [44])
        self.assertEquals(index.indices[2][2], [0, 1, 22, 44])

        index.remove_record_from_index(record)
        self.assertEquals(index.indices[0][244], [])
        self.assertEquals(index.indices[1][24], [])
        self.assertEquals(index.indices[2][2], [0, 1, 22])

    
