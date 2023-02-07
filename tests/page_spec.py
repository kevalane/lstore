import unittest
from lstore.page import Page
#import Page

PAGE_SIZE = 4096
DATA_SIZE = 8   # int64

class PageTestCase(unittest.TestCase):
    def test_init(self):
        page = Page()
        self.assertEqual(page.num_records, 0)
        self.assertEqual(len(page.data), PAGE_SIZE)

    def test_has_capacity(self):
        page = Page()
        self.assertTrue(page.has_capacity())
        for i in range(0, 600):
            if (i == 511):
                self.assertTrue(page.has_capacity())
            if (i > 511):
                with self.assertRaises(Exception):
                    page.write(i*400)
            else:
                page.write(i*400)

        self.assertFalse(page.has_capacity())
    
    def test_write(self):
        page = Page()
        page.write(256)
        self.assertEqual(page.num_records, 1)
        self.assertEqual(page.data[0:8], b'\x00\x00\x00\x00\x00\x00\x01\x00')

        page.write(512)
        self.assertEqual(page.num_records, 2)
        self.assertEqual(page.data[8:16], b'\x00\x00\x00\x00\x00\x00\x02\x00')

        page.write(1353156846)
        self.assertEqual(page.num_records, 3)
        self.assertEqual(page.data[16:24], b'\x00\x00\x00\x00\x50\xA7\x88\xEE')

    

if __name__ == '__main__':
    unittest.main()