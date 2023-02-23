import unittest
from lstore.bufferpool import Bufferpool
from lstore.wide_page import Wide_Page
import shutil
import os

class BufferPoolTest(unittest.TestCase):
    def setUp(self):
        self.bufferpool = Bufferpool(16)
        try:
            os.mkdir('./data')
            os.mkdir('./data/base')
            os.mkdir('./data/tail')
        except:
            pass

    def test_init(self):
        self.assertEqual(self.bufferpool.max_pages, 16)
        self.assertEqual(self.bufferpool.num_pages, 0)
        self.assertEqual(self.bufferpool.base_pages, {})
        self.assertEqual(self.bufferpool.tail_pages, {})

    def test_write_page(self):
        self.bufferpool.base_pages[0] = {
            'semaphore_count': 0,
            'dirty': True,
            'wide_page': Wide_Page(4, 0)
        }
        self.assertTrue(self.bufferpool.write_page(0, True))
        self.assertEqual(self.bufferpool.base_pages[0]['dirty'], False)

    def test_write_page_not_exist(self):
        self.assertFalse(self.bufferpool.write_page(0, True))

    def test_write_no_files(self):
        self.bufferpool.base_pages[0] = {
            'semaphore_count': 0,
            'dirty': True,
            'wide_page': Wide_Page(4, 0)
        }
        shutil.rmtree('./data/base')
        shutil.rmtree('./data/tail')
        self.assertFalse(self.bufferpool.write_page(0, True))
        self.assertEqual(self.bufferpool.base_pages[0]['dirty'], True)
        os.mkdir('./data/base')
        os.mkdir('./data/tail')
    
    def test_write_page_not_dirty(self):
        self.bufferpool.base_pages[0] = {
            'semaphore_count': 0,
            'dirty': False,
            'wide_page': Wide_Page(4, 0)
        }
        self.assertFalse(self.bufferpool.write_page(0, True))
        self.assertEqual(self.bufferpool.base_pages[0]['dirty'], False)

    def test_retrieve_page(self):
        # create wide_page object
        wide_page = Wide_Page(4, 0)

        # update some columns
        wide_page.columns[0].write(153315)
        wide_page.columns[1].write(123)
        wide_page.columns[5].write(1231211)

        self.bufferpool.base_pages[0] = {
            'semaphore_count': 0,
            'dirty': True,
            'wide_page': wide_page
        }

        self.assertEqual(self.bufferpool.base_pages[0]['dirty'], True)
        self.assertTrue(self.bufferpool.write_page(0, True))

        # remove wide_page from bufferpool
        del self.bufferpool.base_pages[0]

        # retrieve wide_page from disk to bufferpool
        ret_page = self.bufferpool.retrieve_page(0, True, 4)
        self.assertEqual(self.bufferpool.base_pages[0]['dirty'], False)
        self.assertEqual(self.bufferpool.base_pages[0]['wide_page']
                        .columns[0].get(0), 153315)
        self.assertEqual(self.bufferpool.base_pages[0]['wide_page']
                        .columns[1].get(0), 123)
        self.assertEqual(self.bufferpool.base_pages[0]['wide_page']
                        .columns[5].get(0), 1231211)

        self.assertEqual(ret_page.columns[0].get(0), 153315)
        self.assertEqual(ret_page.columns[1].get(0), 123)
        self.assertEqual(ret_page.columns[5].get(0), 1231211)
