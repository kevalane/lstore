import unittest
from lstore.db import Database, jsonKeys2int
from lstore.config import *
import os
import json

class DatabaseTestCase(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.assertEquals(self.db.tables, {})

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

    def test_drop_table(self):
        db = Database()
        name = "test_table"
        db.create_table(name, 4, 1)
        self.assertNotEquals(db.tables, {})
        self.assertEquals(db.get_table(name).name, name)

        self.assertEquals(db.drop_table(name), True)
        self.assertEquals(db.tables, {})

    
    # MILESTONE 2
    def test_open(self):
        db = Database()
        path = "test.db"
        db.open(path)

    def test_close(self):
        db = Database()
        path = "test.db"
        db.open(path)
        db.close()

    def test_get_existing_table(self):
        # create a table with a name
        name = "test_table"
        self.db.create_table(name, 4, 0)
        # get the table using the name
        table = self.db.get_table(name)
        # assert that the table is not None
        self.assertIsNotNone(table)
        # assert that the table has the correct name
        self.assertEqual(table.name, name)
    
    def test_get_non_existing_table(self):
        # get a table with a name that does not exist
        name = "non_existing_table"
        table = self.db.get_table(name)
        # assert that the table is None
        self.assertIsNone(table)
    
    def test_get_table_from_disk(self):
        # create a table with a name
        name = "test_table"
        self.db.create_table(name, 4, 0)
        # close the database
        self.db.close()
        # create a new database
        db = Database()
        # get the table using the name
        table = db.get_table(name)
        # assert that the table is not None
        self.assertIsNotNone(table)
        # assert that the table has the correct name
        self.assertEqual(table.name, name)

    def test_int_keys_in_dict(self):
        # create a dictionary with string keys and integer values
        data = {
            "1": 10,
            "2": 20,
            "3": 30
        }
        # convert the keys to integers using the jsonKeys2int function
        converted_data = jsonKeys2int(data)
        # assert that the keys are now integers
        self.assertIsInstance(list(converted_data.keys())[0], int)
        # assert that the values are unchanged
        self.assertEqual(list(converted_data.values())[0], 10)

    def test_int_keys_in_nested_dict(self):
        # create a nested dictionary with string keys and integer values
        data = {
            "1": {
                "2": 20,
                "3": 30
            },
            "4": {
                "5": 50,
                "6": 60
            }
        }
        # convert the keys to integers using the jsonKeys2int function
        converted_data = jsonKeys2int(data)
        # assert that the keys are now integers
        self.assertIsInstance(list(converted_data.keys())[0], int)
        # assert that the nested keys are now integers
        self.assertIsInstance(list(converted_data.values())[0], dict)

    def test_no_int_keys(self):
        # create a dictionary with string keys and values
        data = {
            "one": "1",
            "two": "2",
            "three": "3"
        }
        # convert the keys to integers using the jsonKeys2int function
        converted_data = jsonKeys2int(data)
        # assert that the keys are unchanged
        self.assertIsInstance(list(converted_data.keys())[0], str)
        # assert that the values are unchanged
        self.assertEqual(list(converted_data.values())[0], "1")

    def test_list_jsonkeys(self):
        records = list()
        records.append({
            "1": 10,
            "2": 20,
            "3": 30
        })
        records.append({
            "4": 40,
            "5": 50,
            "6": 60
        })
        # convert the keys to integers using the jsonKeys2int function
        converted_data = jsonKeys2int(records)
        # assert that the keys are now integers
        self.assertIsInstance(list(converted_data[0].keys())[0], int)
        # assert that the values are unchanged
        self.assertEqual(list(converted_data[0].values())[0], 10)

    def tearDown(self):
        # remove the test directory
        if os.path.exists(self.db.path):
            os.system(f"rm -rf {self.db.path}")
