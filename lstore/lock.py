import threading

class Lock:
	
	def __init__(self):
		self.index_locks = {}
		self.rid_locks = {}


	def acquire_rid(self, rid: int) -> bool:
		"""
		Lock a specific record id
		This ensures that no other transaction can access the record
		Locks both the page directory and retrievs the relevant base page
		into the bufferpool. If anything fails, the lock is released.
		:param rid: int  # Record id to lock
		:return: bool	 # True if lock is acquired, False otherwise
		"""
		if rid in self.rid_locks:
			if self.rid_locks[rid].acquire(False):
				return True
			else:
				return False
		else:
			self.rid_locks[rid] = threading.Lock()
			self.rid_locks[rid].acquire()
			return True

	def release_rid(self, rid: int) -> bool:
		"""
		Release the rid lock
		:param rid: int  # Record id to lock
		:return: bool	 # True if lock is released, False otherwise
		"""
		if rid in self.rid_locks:
			self.rid_locks[rid].release()
			return True
		return False # Lock not found

	def acquire_index_lock(self, index_column: int, index_value: int) -> bool:
		"""
		Lock a specific index value in a column
		For query updates(), a lock is required for both all the old indexes
		and the new indexes. This is because the old indexes are being deleted
		and the new indexes are being added.

		:param index_column: int 	  # Index of the column to lock
		:param index_value: int 	  # Value of the index to lock
		:return: bool				  # True if lock is acquired, False otherwise
		"""
		if index_column not in self.index_locks:
			self.index_locks[index_column] = {}

		if index_value in self.index_locks[index_column]:
			if self.index_locks[index_column][index_value].acquire(False):
				return True
			else:
				return False
		else:
			self.index_locks[index_column][index_value] = threading.Lock()
			self.index_locks[index_column][index_value].acquire()
			return True

	def release_index_lock(self, index_column: int, index_value: int) -> bool:
		"""
		Release a specific index value in a column
		:param index_column: int 	  # Index of the column to lock
		:param index_value: int 	  # Value of the index to lock
		:return: bool				  # True if lock is released, False otherwise
		"""
		if index_column in self.index_locks and index_value in self.index_locks[index_column]:
			self.index_locks[index_column][index_value].release()
			return True
		return False