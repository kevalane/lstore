import unittest
from lstore.table import Table, Record
from lstore.wide_page import Wide_Page
from lstore.page import Page
from random import randint, seed
from lstore.config import *

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
        self.assertEqual(type(table.base_pages[0]), Wide_Page)
        self.assertEqual(table.tail_pages, [])
        self.assertEqual(table.rid_generator, 0)
    
    def test_init_tail(self):
        num_cols = 4
        tail = Wide_Page(num_cols, 0)
        self.assertEqual(len(tail.columns), META_COLUMNS + num_cols)
        for col in tail.columns:
            self.assertEqual(type(col), Page)

    def test_init_base(self):
        num_cols = 4
        base = Wide_Page(num_cols, 0)
        self.assertEqual(len(base.columns), META_COLUMNS + num_cols)
        for col in base.columns:
            self.assertEqual(type(col), Page)

    def test_add_record(self):
        key = 0
        table = Table("test", 3, key)
        columns = [1, 2, 3]
        table.add_record(columns)

        self.assertEqual(table.rid_generator, 0) # increment rid_generator
        self.assertEqual(table.page_directory, {1: ('base', 0, 0)})
        self.assertEqual(table.index.indices[0], {1: [1]})

        for col in range(table.num_columns):
            base_page = table.get_base_record(1)
            self.assertEqual(base_page[META_COLUMNS:][col], 
                             columns[col])

        base_page = table.bufferpool.retrieve_page(0, True, 5)
        self.assertEqual(base_page.columns[INDIRECTION_COLUMN].get(0), 1)
        self.assertEqual(base_page.columns[RID_COLUMN].get(0), 1)
        self.assertEqual(base_page.columns[TIMESTAMP_COLUMN].get(0), 0)
        self.assertEqual(base_page.columns[SCHEMA_ENCODING_COLUMN].get(0), 0)

    def test_add_2000_records(self):
        key = 0
        table = Table("test", 4, key)
        seed(134134134)
        for i in range(2000):
            column = [i+1, randint(0, 1000), randint(0, 1000), randint(0, 1000)]
            table.add_record(column)
            RID = table.rid_generator
            if (i % 512 == 0):
                self.assertEqual(table.latest_base_page_index, i // 512)

        for col in range(table.num_columns):
            record = table.get_base_record(575)
            self.assertEqual(record[META_COLUMNS:][col], 
                             [575, 72, 252, 911][col])
            
        base_page = table.bufferpool.retrieve_page(1, True, 5)
        self.assertEqual(base_page.columns[RID_COLUMN].get(0), 513)
        self.assertEqual(base_page.columns[INDIRECTION_COLUMN].get(575 % 513), 575)
        self.assertEqual(base_page.columns[RID_COLUMN].get(575 % 513), 575)
        self.assertEqual(base_page.columns[TIMESTAMP_COLUMN].get(575 % 513), 0)
        self.assertEqual(base_page.columns[SCHEMA_ENCODING_COLUMN].get(575 % 513), 0)


    def test_simple_get(self):
        key = 0
        table = Table("test", 4, key)
        columns = [1, 1, 2, 3]
        columns2 = [2, 4, 5, 6]
        table.add_record(columns)
        table.add_record(columns2)
        self.assertEqual(table.get_record(1), columns)
        self.assertEqual(table.get_record(2), columns2)

    def test_simple_update(self):
        key = 0
        table = Table("test", 4, key)
        columns = [33333331, 1, 2, 3]
        columns2 = [33333332, 4, 5, 6]
        table.add_record(columns)
        table.add_record(columns2)
        table.update_record(33333331, [None, 7, 8, 9])
        self.assertEqual(table.get_record(33333331), [33333331, 7, 8, 9])
        self.assertEqual(table.get_record(33333332), columns2)

    def test_update_with_none(self):
        key = 0
        table = Table("test", 4, key)
        columns = [111111, 1, 2, 3]
        columns2 = [222222, 4, 5, 6]
        table.add_record(columns)
        table.add_record(columns2)
        table.update_record(111111, [None, None, 8, None])
        self.assertEqual(table.get_record(111111), [111111, 1, 8, 3])
        self.assertEqual(table.get_record(222222), columns2)

    def test_get_with_meta(self):
        key = 0
        table = Table("test", 4, key)
        columns = [11, 1, 2, 3]
        columns2 = [22, 4, 5, 6]
        table.add_record(columns)
        table.add_record(columns2)
        self.assertEqual(table.get_record(11, True), [11, 11, 0, 0, 0, 0, 11, 1, 2, 3])
        self.assertEqual(table.get_record(22, True), [22, 22, 0, 0, 0, 0, 22, 4, 5, 6])
        table.update_record(11, [None, None, 8, None])
        self.assertEqual(table.get_record(11, True), [1, 11, 0, 10, 0, 0, 11, 1, 8, 3])

    def test_get_column(self):
        key = 0
        table = Table("test", 4, key)
        columns = [[11, 1, 2, 3], [22, 4, 5, 6], [33, 7, 8, 9], [44, 10, 11, 12]]
        for col in columns:
            table.add_record(col)

        # Test with valid column index
        self.assertEqual(table.get_column(1), [(11, 1), (22, 4), (33, 7), (44, 10)])
        
        # Test with invalid column index
        with self.assertRaises(IndexError):
            table.get_column(5)
        
        # Test with column index 0 (the key column)
        self.assertEqual(table.get_column(0), [(11, 11), (22, 22), (33, 33), (44, 44)])

    def test_delete_fail(self):
        key = 0
        table = Table("test", 4, key)
        columns = [1, 1, 2, 3]
        columns2 = [2, 4, 5, 6]
        table.add_record(columns)
        table.add_record(columns2)
        self.assertFalse(table.delete_record(3))
    
    def test_delete_record_success(self):
        key = 0
        table = Table("test", 4, key)
        columns = [1, 1, 2, 3]
        columns2 = [2, 4, 5, 6]
        table.add_record(columns)
        table.add_record(columns2)
        rid = 1
        self.assertTrue(table.delete_record(rid))
        with self.assertRaises(KeyError):
            table.get_record(rid)
        self.assertEqual(table.get_record(2), columns2)

    def test_delete_record_with_tail_page(self):
        key = 0
        table = Table("test", 4, key)
        columns = [11, 1, 2, 3]
        columns2 = [22, 4, 5, 6]
        table.add_record(columns)
        table.add_record(columns2)
        table.update_record(11, [None, None, None, 9])
        table.update_record(11, [None, 10, None, None])
        table.update_record(11, [None, None, None, 15])
        rid = 11
        self.assertEqual(table.get_tail_page(1), [11, 1, 11, 1, 0, 1, 0, 0, 0, 9])
        self.assertEqual(table.get_tail_page(2), [1, 2, 11, 101, 0, 2, 0, 10, 0, 9])
        self.assertEqual(table.get_tail_page(3), [2, 3, 11, 101, 0, 3, 0, 10, 0, 15])        
        self.assertEqual(table.get_record(11), [11, 10, 2, 15])
        self.assertTrue(table.delete_record(rid))
        
        with self.assertRaises(KeyError):
                table.get_record(rid)
        
        self.assertEqual(table.get_base_record(-11), [3, 11, 0, 101, 0, 0, 11, 1, 2, 3])
        self.assertEqual(table.get_tail_page(-1), [11, 1, 11, 1, 0, 1, 0, 0, 0, 9])
        self.assertEqual(table.get_tail_page(-2), [1, 2, 11, 101, 0, 2, 0, 10, 0, 9])
        self.assertEqual(table.get_tail_page(-3), [2, 3, 11, 101, 0, 3, 0, 10, 0, 15])
        self.assertEqual(table.get_record(22), columns2)

    def test_update_all_records(self):
        key = 0
        table = Table("test", 4, key)
        seed(134134)
        updated_cols = []
        # Insert 1000 records
        for i in range(1000):
            columns = [34432332+i+1, randint(0, 1000), randint(0, 1000), randint(0, 1000)]
            table.add_record(columns)

        # Update all 1000 records
        for i in range(1000):
            new_columns = [None, randint(1001, 2000), randint(1001, 2000), randint(1001, 2000)]
            table.update_record(i+1, new_columns)
            updated_cols.append([34432332+i+1, new_columns[1], new_columns[2], new_columns[3]])

        # Check if all records have the new column values
        for i in range(1000):
            record = table.get_record(34432332+i+1)
            expected_columns = updated_cols[i]
            self.assertEqual(record, expected_columns)