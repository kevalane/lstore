from lstore.index import Index
from time import time

# these refer to the colum indices of a record
# this means the first 4 values of the record will be metadata
# if you add more metadata columns, probably best to add on to these values
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

# this is the most open-ended class because he doesn't provide much to go off of.
# there's like a million ways to implement this
# below I suggest one way it could be implemented

# might be better to have this in a separate file, although i guess it could work in just one file
class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    # SUGGESTED METHODS

    # set meta_data info
    # consider making a separate get date function to be able to set the timestamp value
    def set_meta_data(self):
        pass

    # set column info (might wanna call set_meta_data when you do this, so metadata can be updated when records are updated)
    def set_columns(self, cols):
        pass

    # set rid value
    def set_rid(self, rid):
        pass

    # set key value
    def set_key(self, key):
        pass

    # print all the columns (makes up the record)
    def print_record(self):
        pass

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)

        # OTHER THINGS TO POSSIBLY INITIALIZE

        # column offset initialize to 4 (offsets the columns to not include the metadata columns)
        #   initialize to higher if you add more metadata columns (consider making a macro as well)
        # page ranges (a table is made up of page ranges)
        #   might want to make a page range class as well (helps with abstraction)
        # call create_index for all the columns you have so far (metadata columns)
        pass

    # SUGGESTED METHODS

    def insert_record(self, key, columns):
        pass

    def get_record(self, primary_key, key):
        pass

    def get_multiple_records(self, search_key, search_key_index):
        pass

    def delete_record(self, key):
        pass

    def update_record(self, primary_key, columns):
        pass

    # to help implement query.sum later
    def sum(self, start, end, column):
        pass


    def __merge(self):
        print("merge is happening")
        pass
