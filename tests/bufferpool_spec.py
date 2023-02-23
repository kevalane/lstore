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

    def test_retrieve_page_already_in_bufferpool(self):

        # create wide_page object
        wide_page = Wide_Page(4, 0)

        inserted_obj = {
            'semaphore_count': 0,
            'dirty': False,
            'wide_page': wide_page
        }
        self.bufferpool.base_pages[0] = inserted_obj
        self.assertEqual(self.bufferpool.retrieve_page(0, True, 4), wide_page)

    def test_retrieve_page_not_exist(self):
        try:
            os.remove('./data/base/0.json')
            del self.bufferpool.base_pages[0]
        except:
            print()
        return_obj = self.bufferpool.retrieve_page(0, True, 4)
        self.assertEqual(return_obj, None)

    def test_pin_base_page(self):
        # Add a page to the base pages
        wide_page = Wide_Page(4, 0)
        self.bufferpool.base_pages[0] = {
            'semaphore_count': 0,
            'dirty': False,
            'wide_page': wide_page
        }

        # Pin the page and check that the semaphore count is increased
        self.assertTrue(self.bufferpool.pin(0, True))
        self.assertEqual(self.bufferpool.base_pages[0]['semaphore_count'], 1)

    def test_pin_tail_page(self):
        # Add a page to the tail pages
        wide_page = Wide_Page(4, 1)
        self.bufferpool.tail_pages[0] = {
            'semaphore_count': 0,
            'dirty': False,
            'wide_page': wide_page
        }

        # Pin the page and check that the semaphore count is increased
        self.assertTrue(self.bufferpool.pin(0, False))
        self.assertEqual(self.bufferpool.tail_pages[0]['semaphore_count'], 1)

    def test_unpin_base_page(self):
        # Add a page to the base pages
        wide_page = Wide_Page(4, 0)
        self.bufferpool.base_pages[0] = {
            'semaphore_count': 1,
            'dirty': False,
            'wide_page': wide_page
        }

        # Unpin the page and check that the semaphore count is decreased
        self.assertTrue(self.bufferpool.unpin(0, True))
        self.assertEqual(self.bufferpool.base_pages[0]['semaphore_count'], 0)

    def test_unpin_tail_page(self):
        # Add a page to the tail pages
        wide_page = Wide_Page(4, 1)
        self.bufferpool.tail_pages[0] = {
            'semaphore_count': 1,
            'dirty': False,
            'wide_page': wide_page
        }

        # Unpin the page and check that the semaphore count is decreased
        self.assertTrue(self.bufferpool.unpin(0, False))
        self.assertEqual(self.bufferpool.tail_pages[0]['semaphore_count'], 0)

    def test_pin_unpin_base_page_not_exist(self):
        # Unpin a page that does not exist
        self.assertFalse(self.bufferpool.unpin(0, True))
        self.assertFalse(self.bufferpool.pin(0, True))

    def test_mark_dirty_base_page(self):
        # Add a page to the base pages
        wide_page = Wide_Page(4, 0)
        self.bufferpool.base_pages[0] = {
            'semaphore_count': 0,
            'dirty': False,
            'wide_page': wide_page
        }

        # Mark the page as dirty and check that the 'dirty' flag is updated
        self.assertTrue(self.bufferpool.mark_dirty(0, True))
        self.assertEqual(self.bufferpool.base_pages[0]['dirty'], True)

    def test_mark_dirty_tail_page(self):
        # Add a page to the tail pages
        wide_page = Wide_Page(4, 1)
        self.bufferpool.tail_pages[0] = {
            'semaphore_count': 0,
            'dirty': False,
            'wide_page': wide_page
        }

        # Mark the page as dirty and check that the 'dirty' flag is updated
        self.assertTrue(self.bufferpool.mark_dirty(0, False))
        self.assertEqual(self.bufferpool.tail_pages[0]['dirty'], True)

    def test_mark_dirty_nonexistent_page(self):
        # Mark a non-existent page as dirty and check that the method returns False
        self.assertFalse(self.bufferpool.mark_dirty(0, True))
        self.assertFalse(self.bufferpool.mark_dirty(0, False))

