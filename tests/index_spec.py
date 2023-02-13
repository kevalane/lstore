import unittest
from lstore.index import Index
from lstore.table import Table, Record

class IndexTestCase(unittest.TestCase):
    def test_init(self):
        table = Table("test", 3, 0)
        index = Index(table)
        self.assertEquals(index.indices, {})

    def test_create_index(self):
        table = Table("test", 3, 0)
        index = Index(table)
        col = 0
        self.assertTrue(index.create_index(col))
        self.assertFalse(index.create_index(col))
        self.assertEquals(index.indices[col], {})

    def test_push_record_to_index(self):
        # setup
        table = Table("test2", 3, 0)
        index = Index(table)
        col = 0
        index.create_index(col)
        self.assertTrue(index.create_index(col))
        self.assertFalse(index.create_index(col))
        self.assertEquals(index.indices[col], {})

        # test
        record = Record(0, 0, [0, 1, 2])
        index.push_record_to_index(record)
        self.assertEquals(index.indices[col][1], [0])