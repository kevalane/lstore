import unittest
from lstore.record import Record
from lstore.config import *

class RecordTestCase(unittest.TestCase):

    def test_record_initialization(self):
        key = 123
        columns = [1, 2, 3, 4]
        rid = 456
        record = Record(key, columns, rid)

        self.assertEqual(record.key, key)
        self.assertEqual(record.columns, columns)
        self.assertEqual(record.rid, rid)

    def test_record_getitem(self):
        key = 123
        columns = [1, 2, 3, 4]
        record = Record(key, columns)

        self.assertEqual(record[0], 1)
        self.assertEqual(record[1], 2)
        self.assertEqual(record[2], 3)
        self.assertEqual(record[3], 4)

    def test_record_str(self):
        key = 123
        columns = [1, 2, 3, 4]
        record = Record(key, columns)

        self.assertEqual(str(record), str(columns))

    def test_record_get(self):
        key = 123
        columns = [1, 2, 3, 4]
        record = Record(key, columns)

        self.assertEqual(record.__get__(), columns)
