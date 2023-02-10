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