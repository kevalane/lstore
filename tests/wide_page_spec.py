import unittest
from lstore.wide_page import Wide_Page

META_COLUMNS = 4

class Wide_Page_Test(unittest.TestCase):

    def setUp(self):
        wide_page = Wide_Page(5, 0)
        self.assertEqual(wide_page.key_index, 0)
        self.assertEqual(len(wide_page.columns), META_COLUMNS + 5)

    def test_init(self):
        wide_page = Wide_Page(1, 0)
        self.assertEqual(wide_page.key_index, 0)
        self.assertEqual(len(wide_page.columns), META_COLUMNS + 1)
        
    def test_write_to_disk(self):
        wide_page = Wide_Page(1, 0)
        wide_page.write_to_disk(0, True)

    