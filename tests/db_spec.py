import unittest
from lstore.db import Database

class DatabaseTestCase(unittest.TestCase):
    def test_init(self):
        db = Database()
        self.assertEquals(db.tables, {})

    def test_create_table(self):
        db = Database()
        name = "test_table"
        db.create_table(name, 4, 1)
        self.assertNotEquals(db.tables, {})
        self.assertEquals(db.get_table(name).name, name)

    def test_create_with_same_name(self):
        db = Database()
        name = "test_table"
        table1 = db.create_table(name, 4, 1)
        self.assertNotEquals(db.tables, {})
        self.assertEquals(db.get_table(name), table1)

        table2 = db.create_table(name, 3, 1)
        self.assertEquals(db.get_table(name), table1)
        self.assertEquals(table2, None) # should return None

    def test_drop_empty_database(self):
        db = Database()
        name = "test_table"
        self.assertEquals(db.drop_table(name), False)
