import unittest
from lstore.bufferpool import Bufferpool
from lstore.wide_page import Wide_Page
import shutil
import os

MAX_PAGES = 16

class BufferPoolTest(unittest.TestCase):
    def setUp(self):
        self.bufferpool = Bufferpool(MAX_PAGES)
        try:
            os.mkdir('./data')
            os.mkdir('./data/base')
            os.mkdir('./data/tail')
        except:
            pass

    def test_init(self):
        self.assertEqual(self.bufferpool.max_pages, MAX_PAGES)
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

    def test_retrieve_max_pages(self):
        for i in range(MAX_PAGES):
            wide_page = Wide_Page(4, 0)
            self.bufferpool.base_pages[i] = {
                'semaphore_count': 0,
                'dirty': False,
                'wide_page': wide_page
            }
            self.bufferpool.num_pages += 1
            self.bufferpool.deque.append({
                'index': i,
                'base_page': True
            })
        
        seventeenth = Wide_Page(4, 0)
        seventeenth.columns[0].write(123)
        seventeenth.write_to_disk(44, False)
        self.assertIsNotNone(self.bufferpool.retrieve_page(44, False, 4))
        self.assertEqual(self.bufferpool.retrieve_page(44, False, 4).columns[0].get(0), 
                        seventeenth.columns[0].get(0))
        self.assertEqual(self.bufferpool.retrieve_page(44, False, 4).columns[0].get(0), 123)

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

    def test_evict_all_pinned(self):
        # Create a base page with index 0
        wide_page = Wide_Page(4, 0)
        self.bufferpool.base_pages[0] = {
            'semaphore_count': 1,
            'dirty': False,
            'wide_page': wide_page
        }
        self.bufferpool.deque.append({
            'index': 0,
            'base_page': True
        })

        # Try to evict the page and check that the method returns False
        self.assertFalse(self.bufferpool.evict())

    def test_evict(self):
        # Fill up the buffer pool with pages
        for i in range(MAX_PAGES):
            wide_page = Wide_Page(4, i)
            self.bufferpool.base_pages[i] = {
                'semaphore_count': 0,
                'dirty': False,
                'wide_page': wide_page
            }
            self.bufferpool.num_pages += 1
            self.bufferpool.deque.append({
                'index': i,
                'base_page': True
            })

        # Evict a page from the buffer pool
        self.assertTrue(self.bufferpool.evict())
        self.assertNotIn(0, self.bufferpool.base_pages)

        self.assertNotIn(0, [page['index'] for page in self.bufferpool.deque])
        self.assertEqual(self.bufferpool.num_pages, MAX_PAGES-1)

        # Evict all pages from the buffer pool
        for i in range(MAX_PAGES - 1):
            self.assertTrue(self.bufferpool.evict())
        self.assertEqual(self.bufferpool.num_pages, 0)
        self.assertFalse(self.bufferpool.evict())


    def test_touch_page(self):
        # Create a base page with index 0
        wide_page = Wide_Page(4, 0)
        self.bufferpool.base_pages[0] = {
            'semaphore_count': 0,
            'dirty': False,
            'wide_page': wide_page
        }
        self.bufferpool.deque.append({
            'index': 0,
            'base_page': True
        })

        # Touch the base page with index 0
        self.assertTrue(self.bufferpool.touch_page(0, True))
        self.assertEqual(self.bufferpool.deque[-1]['index'], 0)

        # Create a tail page with index 1
        wide_page = Wide_Page(4, 1)
        self.bufferpool.tail_pages[1] = {
            'semaphore_count': 0,
            'dirty': False,
            'wide_page': wide_page
        }
        self.bufferpool.deque.append({
            'index': 1,
            'base_page': False
        })

        # now our new tail page should be most recent
        self.assertEqual(self.bufferpool.deque[-1]['index'], 1)

        # touch first one again
        self.assertTrue(self.bufferpool.touch_page(0, True))
        self.assertEqual(self.bufferpool.deque[-1]['index'], 0)

    def test_touch_page_fail(self):
        # Touch a non-existent page
        self.assertFalse(self.bufferpool.touch_page(2, True))
        self.assertFalse(self.bufferpool.touch_page(2, False))

        # Create a base page with index 0
        wide_page = Wide_Page(4, 0)
        self.bufferpool.base_pages[0] = {
            'semaphore_count': 0,
            'dirty': False,
            'wide_page': wide_page
        }
        # let's not append it to deque
        self.assertFalse(self.bufferpool.touch_page(0, True))

    def test_retrieve_page_all_pinned(self):
        # fill up the buffer pool with pinned pages
        for i in range(MAX_PAGES):
            wide_page = Wide_Page(4, i)
            self.bufferpool.base_pages[i] = {
                'semaphore_count': 1,
                'dirty': False,
                'wide_page': wide_page
            }
            self.bufferpool.num_pages += 1
            self.bufferpool.deque.append({
                'index': i,
                'base_page': True
            })
        
        # try to retrieve a page
        self.assertFalse(self.bufferpool.retrieve_page(17, True, 5))