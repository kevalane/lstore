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
        
        # convert value to bytes
        value_to_bytes = value.to_bytes(DATA_SIZE, 
                                        byteorder=STORAGE_OPTION, 
                                        signed=False)

        # calc offset
        offset = self.num_records * DATA_SIZE

        # write the value to the page
        self.data[offset:offset+DATA_SIZE] = value_to_bytes

        # increment num_records
        self.num_records += 1

        return True


