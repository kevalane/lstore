from lstore.index import Index
from lstore.page import Page
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, key, columns, rid=None):
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
    
    def add_page(self, columns):
        """
        :param columns: list    # List of column values
        """
        if not self.columns[0].has_capacity():
            self.base_pages.append(Base_Page(len(columns), key_index))
        # first, insert the metadata
        self.columns[0].write(None) # UPDATE # INDIRECTION COLUMN
        self.columns[1].write(record.rid) # RID COLUMN # UPDATE
        self.columns[2].write(0) # TIMESTAMP COLUMN
        self.columns[3].write(0) # SCHEMA ENCODING COLUMN
        for page in self.columns:
            page.write()


class Tail_Page:

    def __init__(self, num_columns, key_index):
        key_index = key_index
        self.columns = []
        for col in range(num_columns+4):
            # Add a column for every column being added, plus 4 for the metadata columns
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
        self.rid_generator = 0 #Keeps track of the RID to be generated each time a record is added
        pass

    def get_record(self, rid):
        pass
        # Returns a record object

    def delete_record(self, rid):
        pass

    def get_column(self, column):
        # The column parameter will be an index for which column is to be retrieved
        pass

    def update_record(self, record):
        pass

    def add_record(self, columns):
        """
        :param record: list    # List of column values
        """
        pass

    def assign_rid(self):
        """
        Keeps track of creating new RIDs for new records
        """
        rid = self.rid_generator
        self.rid_generator += 1
        return rid

    '''
    MERGE WILL BE IMPLEMENTED IN MILESTONE 2
    def __merge__(self):
        print("merge is happening")
        pass
    '''
