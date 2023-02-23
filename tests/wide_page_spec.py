import unittest
from lstore.wide_page import Wide_Page

class Wide_Page_Test(unittest.TestCase):
    def test_write_to_disk(self):
        wide_page = Wide_Page(1, 0)
        wide_page.write_to_disk(0, True)