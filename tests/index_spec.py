import unittest
from lstore.index import Index
from lstore.table import Table

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