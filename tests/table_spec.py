import unittest
from lstore.table import Table, Record, Base_Page, Tail_Page
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
META_COLUMNS = 4

class TableTestCase(unittest.TestCase):
    def test_init_table(self):
        key = 0
        columns = 3
        table = Table("test", columns, key)
        self.assertEqual(table.name, "test")
        self.assertEqual(table.key, key)
        self.assertEqual(table.num_columns, columns)
        self.assertEqual(table.page_directory, {})
        self.assertEqual(table.index.indices, {key: {}}, "Table should create index for primary key column on init")
        self.assertEqual(type(table.base_pages[0]), Base_Page)
        self.assertEqual(table.tail_pages, [])
        self.assertEqual(table.rid_generator, 0)
    
    def test_init_tail(self):
        num_cols = 4
        tail = Tail_Page(num_cols, 0)
        self.assertEqual(len(tail.columns), META_COLUMNS + num_cols)
        for col in tail.columns:
            self.assertEqual(type(col), Page)

    def test_init_base(self):
        num_cols = 4
        base = Base_Page(num_cols, 0)
        self.assertEqual(len(base.columns), META_COLUMNS + num_cols)
        for col in base.columns:
            self.assertEqual(type(col), Page)

    def test_add_record(self):
        key = 0
        table = Table("test", 3, key)
        columns = [1, 2, 3]
        table.add_record(columns)

        self.assertEqual(table.rid_generator, 1) # increment rid_generator
        self.assertEqual(table.page_directory, {1: ('base', 0, 0)})
        self.assertEqual(table.index.indices[0], {1: [1]})

        for col in range(table.num_columns):
            self.assertEqual(table.base_pages[0]
                             .columns[META_COLUMNS + col]
                             .get(0), 
                             columns[col])

        self.assertEqual(table.base_pages[0].columns[INDIRECTION_COLUMN].get(0), 0)
        self.assertEqual(table.base_pages[0].columns[RID_COLUMN].get(0), 1)
        self.assertEqual(table.base_pages[0].columns[TIMESTAMP_COLUMN].get(0), 0)
        self.assertEqual(table.base_pages[0].columns[SCHEMA_ENCODING_COLUMN].get(0), 0)

    