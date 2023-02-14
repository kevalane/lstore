from lstore.index import Index
from lstore.page import Page
from lstore.record import Record
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

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
            self.add_page()

    def add_page(self):
        self.columns.append(Page())


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

    def update_record(self, rid, new_cols):
        pass

    def add_record(self, columns):
        """
        :param columns: list    # List of column values
        """
        if not self.columns[0].has_capacity():
            self.base_pages.append(Base_Page(len(columns), self.key))
            self.add_record(columns)
        # first, create a record object from the columns
        rid = self.assign_rid()
        record = Record(self.key, columns, rid)
        # next, add the metadata to columns
        self.columns[0].write(0) # UPDATE # INDIRECTION COLUMN
        self.columns[1].write(rid) # RID COLUMN
        self.columns[2].write(0) # TIMESTAMP COLUMN
        self.columns[3].write(0) # SCHEMA ENCODING COLUMN
        for index, item in enumerate(columns):
            self.base_pages[-1][index+4].write(item)
        # finally add this rid to the page directory with a tuple containing the page it's found and the index within that page
        location = (len(self.base_pages)-1, self.base_pages[-1].num_records-1)
        self.page_directory[rid] = location

    def assign_rid(self):
        """
        Keeps track of creating new RIDs for new records
        """
        self.rid_generator += 1
        rid = self.rid_generator
        return rid

    '''
    MERGE WILL BE IMPLEMENTED IN MILESTONE 2
    def __merge__(self):
        print("merge is happening")
        pass
    '''
