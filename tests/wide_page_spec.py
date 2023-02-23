import unittest
from lstore.wide_page import Wide_Page
import os
import shutil

META_COLUMNS = 4

class Wide_Page_Test(unittest.TestCase):

    def setUp(self):
        self.wide_page = Wide_Page(5, 0)
        self.assertEqual(self.wide_page.key_index, 0)
        self.assertEqual(len(self.wide_page.columns), META_COLUMNS + 5)
        try:
            os.mkdir('./data/base')
            os.mkdir('./data/tail')
        except:
            pass

    def test_init(self):
        wide_page = Wide_Page(1, 0)
        self.assertEqual(wide_page.key_index, 0)
        self.assertEqual(len(wide_page.columns), META_COLUMNS + 1)

    def test_write_to_disk(self):
        self.assertTrue(self.wide_page.write_to_disk(0, True))
        self.assertTrue(self.wide_page.write_to_disk(0, False))

    def test_write_to_disk_fail(self):
        return
        # remove directories
        shutil.rmtree('./data/base')
        shutil.rmtree('./data/tail')
        self.assertFalse(self.wide_page.write_to_disk(0, True))
        self.assertFalse(self.wide_page.write_to_disk(0, False))
        os.mkdir('./data/base')
        os.mkdir('./data/tail')

    def test_read_from_disk(self):
        # make offset valid
        self.wide_page.columns[5].num_records = 55
        self.wide_page.columns[6].num_records = 55
        self.wide_page.columns[7].num_records = 55
        self.wide_page.columns[8].num_records = 55

        # insert values into base page
        self.wide_page.columns[5].put(134, 11)
        self.wide_page.columns[6].put(135, 22)
        self.wide_page.columns[7].put(136, 33)
        self.wide_page.columns[8].put(137, 44)
        self.assertTrue(self.wide_page.write_to_disk(14, True))

        # validify offset
        self.wide_page.columns[5].num_records = 512
        self.wide_page.columns[6].num_records = 512
        self.wide_page.columns[7].num_records = 512
        self.wide_page.columns[8].num_records = 512

        # write different values to tail
        self.wide_page.columns[5].put(134, 111)
        self.wide_page.columns[6].put(135, 222)
        self.wide_page.columns[7].put(136, 333)
        self.wide_page.columns[8].put(1354135135, 444)
        print(self.wide_page.columns[5].get(111))
        self.assertTrue(self.wide_page.write_to_disk(32, False))

        read_page = Wide_Page(5, 0)
        self.assertTrue(read_page.read_from_disk(14, True))
        self.assertEquals(read_page.columns[5].get(11), 134)
        self.assertEquals(read_page.columns[6].get(22), 135)
        self.assertEquals(read_page.columns[7].get(33), 136)
        self.assertEquals(read_page.columns[8].get(44), 137)

        self.assertTrue(read_page.read_from_disk(32, False))
        self.assertEquals(read_page.columns[5].get(111), 134)
        self.assertEquals(read_page.columns[6].get(222), 135)
        self.assertEquals(read_page.columns[7].get(333), 136)
        self.assertEquals(read_page.columns[8].get(444), 1354135135)
    

    