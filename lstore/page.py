DATA_SIZE = 8   # int64
PAGE_SIZE = 4096
STORAGE_OPTION = 'big'

"""
Class to represent a page in the database
"""
class Page:
    """
    Constructor for the Page class
    Initializes the number of records to 0 and the data 
    to a bytearray of size PAGE_SIZE, where PAGE_SIZE is
    a constant number of bytes.
    """
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    """
    @returns    True if there is space to insert a record, 
                False otherwise
    """
    def has_capacity(self):
        return self.num_records*DATA_SIZE < PAGE_SIZE

    """
    Writes a value to the page
    :param  value: int64        The value to write to the page

    @returns    True if the value was written to the page,
                False otherwise
    """
    def write(self, value):
        if (self.has_capacity() == False):
            return False
        
        self._insert(value, self.num_records)

        # increment num_records
        self.num_records += 1

        return True

    """
    Updates a value in the page on a given offset
    :param  value: int64        The value to write to the page
    :param  offset: int         The offset to write the value to

    @returns    True if the value was written to the page,
                False otherwise
    """
    def put(self, value, offset):
        if (self._valid_offset(offset) == False):
            return False

        self._insert(value, offset)

        return True

    """
    Deletes a value in the page on a given offset (sets to 0)
    :param  offset: int         The offset to write the value to

    @returns    True if the value 0 was written to the given field,
                False otherwise
    """
    def delete(self, offset):
        if (self._valid_offset(offset) == False):
            return False

        self._insert(0, offset)

        return True

        
    

    """
    Inserts and converts value into the page at the given offset
    :param  value: int64        The value to write to the page
    :param  offset: int         The offset to write the value to
    """
    def _insert(self, value, offset):
        # convert value to bytes
        value_to_bytes = value.to_bytes(DATA_SIZE, 
                                        byteorder=STORAGE_OPTION, 
                                        signed=False)

        # calc offset
        byte_offset = offset * DATA_SIZE

        # write the value to the page
        self.data[byte_offset:byte_offset+DATA_SIZE] = value_to_bytes

    """
    Checks if the given offset is valid
    :param  offset: int         The offset to write the value to

    @returns   True if the offset is valid, False otherwise
    """
    def _valid_offset(self, offset):
        if (offset < 0 or offset >= PAGE_SIZE/DATA_SIZE):
            return False # out of bounds offset

        if (offset >= self.num_records):
            return False # offset outside of current records



