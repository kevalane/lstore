import struct

DATA_SIZE = 8   # int64
PAGE_SIZE = 4096
STORAGE_OPTION = 'big'

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records*DATA_SIZE < PAGE_SIZE

    def write(self, value):
        if (self.has_capacity() == False):
            raise Exception("Page is full")
        
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


