from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Base_Page:
    """
    :param num_columns: string  # Number of columns in the table
    :param key_index: int       # Index of the key column
     """
    def __init__(self, num_columns, key_index):
        self.key_index = key_index
        self.columns = []
        for col in range(num_columns+4):
            # Add a column for every column being added, plus 4 for the metadata columns
            self.columns.append(Page())

    def add_record(self, record):
        """
        :param record: Record object    # The record to be inserted
        """
        if not self.columns[0].has_capacity():
            return False
        # first, insert the metadata
        self.columns[INDIRECTION_COLUMN].write(None) # UPDATE
        self.columns[RID_COLUMN].write(record.rid)
        self.columns[TIMESTAMP_COLUMN].write(None) # UPDATE
        self.columns[SCHEMA_ENCODING_COLUMN].write(0000)
        for page in self.columns:
            page.write(record)


class Tail_Page:

    def __init__(self, num_columns, key_index):
        key_index = key_index
        self.columns = []
        for col in range(num_columns):
            self.columns.append(Page())

class Table:

    """
    :param name: string         # Table name
    :param num_columns: int     # Number of Columns: all columns are integer
    :param key_index: int       # Index of table key in columns
    :param page_directory: dict # Directory of all base and tail pages
    :param index: object        # Index object
    """
    def __init__(self, name, num_columns, key_index):
        self.name = name
        self.key = key_index
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.base_pages = [Base_Page(num_columns, key_index)]
        self.tail_pages = []
        pass

    '''
    MERGE WILL BE IMPLEMENTED IN MILESTONE 2
    def __merge__(self):
        print("merge is happening")
        pass
    '''
