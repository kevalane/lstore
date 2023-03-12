import unittest

from lstore.lock import Lock

class LockTestCase(unittest.TestCase):
	def setUp(self):
		self.lock = Lock()

	def test_acquire_rid(self):
		rid = 1
		self.assertTrue(self.lock.acquire_rid(rid))
		self.assertFalse(self.lock.acquire_rid(rid))
		self.lock.release_rid(rid)
		self.assertTrue(self.lock.acquire_rid(rid))

	def test_release_rid(self):
		rid = 1
		self.assertTrue(self.lock.acquire_rid(rid))
		self.lock.release_rid(rid)
		self.assertTrue(self.lock.acquire_rid(rid))

	def test_release_fail(self):
		rid = 1
		self.assertFalse(self.lock.release_rid(rid))

	def test_acquire_index(self):
		index_column = 1
		index_value = 23
		self.assertTrue(self.lock.acquire_index(index_column, index_value))
		self.assertFalse(self.lock.acquire_index(index_column, index_value))
		self.lock.release_index(index_column, index_value)
		self.assertTrue(self.lock.acquire_index(index_column, index_value))

	def test_release_index_fail(self):
		index_column = 1
		index_value = 23
		self.assertFalse(self.lock.release_index(index_column, index_value))