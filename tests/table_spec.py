import unittest
from lstore.table import Table, Record, Base_Page, Tail_Page

class TableTestCase(unittest.TestCase):
    def test_init(self):
        key = 0
        columns = 3
        table = Table("test", columns, key)
        self.assertEqual(table.name, "test")
        self.assertEqual(table.key, key)
        self.assertEqual(table.num_columns, columns)
        self.assertEqual(table.page_directory, {})
        self.assertEqual(table.index, None) #something
        self.assertEqual(table.tail_pages, [])
        self.assertEqual(len(table.base_pages), columns)
