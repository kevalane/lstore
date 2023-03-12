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