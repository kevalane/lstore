from lstore.config import *

"""
Class to represent a page in the database
"""
class Page:

    def __init__(self) -> None:
        """
        Constructor for the Page class
        Initializes the number of records to 0 and the data 
        to a bytearray of size PAGE_SIZE, where PAGE_SIZE is
        a constant number of bytes.
        """
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self) -> bool:
        """
        @returns    True if there is space to insert a record, 
                    False otherwise
        """
        return self.num_records*DATA_SIZE < PAGE_SIZE

    def write(self, value: int) -> bool:
        """
        Writes a value to the page
        :param  value: int64        The value to write to the page

        @returns    True if the value was written to the page,
                    False otherwise
        """
        if not self.has_capacity():
            return False
        
        self._insert(value, self.num_records)

        # increment num_records
        self.num_records += 1

        return True

    def put(self, value: int, offset: int) -> bool:
        """
        Updates a value in the page on a given offset
        :param  value: int64        The value to write to the page
        :param  offset: int         The offset to write the value to

        @returns    True if the value was written to the page,
                    False otherwise
        """
        if not self._valid_offset(offset):
            return False

        self._insert(value, offset)

        return True

    def delete(self, offset: int) -> bool:
        """
        Deletes a value in the page on a given offset (sets to 0)
        :param  offset: int         The offset to write the value to

        @returns    True if the value 0 was written to the given field,
                    False otherwise
        """
        return self.put(0, offset)

    def get(self, offset: int) -> int:
        """
        Gets value stored in the page at the given offset
        :param  offset: int         The offset to check

        @returns    The value stored in the page at the given offset,
                    -1 if the offset is invalid
        """
        if not self._valid_offset(offset):
            return -1

        # calc offset
        byte_offset = offset * DATA_SIZE

        # read the value from the page
        value = int.from_bytes(self.data[byte_offset:byte_offset+DATA_SIZE], 
                               byteorder=STORAGE_OPTION, 
                               signed=False)

        return value
    
    def _insert(self, value: int, offset: int) -> None:
        """
        Inserts and converts value into the page at the given offset
        :param  value: int64        The value to write to the page
        :param  offset: int         The offset to write the value to
        """
        # convert value to bytes
        value_to_bytes = value.to_bytes(DATA_SIZE, 
                                        byteorder=STORAGE_OPTION, 
                                        signed=False)

        # calc offset
        byte_offset = offset * DATA_SIZE

        # write the value to the page
        self.data[byte_offset:byte_offset+DATA_SIZE] = value_to_bytes

    def _valid_offset(self, offset: int) -> bool:
        """
        Checks if the given offset is valid
        :param  offset: int         The offset to write the value to

        @returns   True if the offset is valid, False otherwise
        """
        if offset < 0 or offset >= PAGE_SIZE/DATA_SIZE:
            return False # out of bounds offset

        if (offset >= self.num_records):
            return False # offset outside of current records

        return True