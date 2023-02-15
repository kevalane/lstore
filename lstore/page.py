DATA_SIZE       =    8       # int64
PAGE_SIZE       =    4096
STORAGE_OPTION  =    'big'

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
    def __init__(self) -> None:
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    """
    @returns    True if there is space to insert a record, 
                False otherwise
    """
    def has_capacity(self) -> bool:
        return self.num_records*DATA_SIZE < PAGE_SIZE

    """
    Writes a value to the page
    :param  value: int64        The value to write to the page

    @returns    True if the value was written to the page,
                False otherwise
    """
    def write(self, value: int) -> bool:
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
    def put(self, value: int, offset: int) -> bool:
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
    def delete(self, offset: int) -> bool:
        return self.put(0, offset)

    """
    Gets value stored in the page at the given offset
    :param  offset: int         The offset to check

    @returns    The value stored in the page at the given offset,
                -1 if the offset is invalid
    """
    def get(self, offset: int) -> int:
        if (self._valid_offset(offset) == False):
            return -1

        # calc offset
        byte_offset = offset * DATA_SIZE

        # read the value from the page
        value = int.from_bytes(self.data[byte_offset:byte_offset+DATA_SIZE], 
                               byteorder=STORAGE_OPTION, 
                               signed=False)

        return value
    

    """
    Inserts and converts value into the page at the given offset
    :param  value: int64        The value to write to the page
    :param  offset: int         The offset to write the value to
    """
    def _insert(self, value: int, offset: int) -> None:
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
    def _valid_offset(self, offset: int) -> bool:
        if (offset < 0 or offset >= PAGE_SIZE/DATA_SIZE):
            return False # out of bounds offset

        if (offset >= self.num_records):
            return False # offset outside of current records

        return True