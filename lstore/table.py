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
        self.index.create_index(key_index)
        self.base_pages = [Base_Page(num_columns, key_index)]
        self.tail_pages = []
        self.rid_generator = 0 #Keeps track of the RID to be generated each time a record is added

    def get_record(self, rid):
        """
        :param rid: int:        # RID of record to be retrieved
        """
        if self.page_directory[rid][0] == 'base':
            page = self.base_pages
        else:
            page = self.tail_pages
        page_num = self.page_directory[rid][1]
        offset = self.page_directory[rid][2]
        update = self.base_pages[page][3].get(offset) #search schema encoding column to retrieve the most recent record
        if update == 1:
            self.get_record(page[page_num][0].get(offset))
        vals = []
        for column in range(len(page[page_num].columns)):
            vals.append(page[page_num].columns[column].get(offset))
        return vals[:4] #return values except the first 4 metadata columns

    def delete_record(self, rid):
        pass

    def get_column(self, column):
        """
        :param column: int      # Index of column to be retrieved
        """
        pass

    def update_record(self, rid, new_cols):
        """
        :param new_cols: list   # List of new column values
        :param rid: int         # RID of the previous record being updates
        """
        if not self.tail_pages.columns[0].has_capacity():
            self.tail_pages.append(Tail_Page(len(new_cols), self.key))
            self.update_record(rid, new_cols)
        # create a record object
        rid = self.assign_rid()
        record = Record(self.key, new_cols, rid)
        # add metadata to columns
        self.tail_pages[-1].columns[0].write(0) # INDIRECTION COLUMN
        self.tail_pages[-1].columns[1].write(rid) # RID COLUMN
        self.tail_pages[-1].columns[2].write(0) # TIMESTAMP COLUMN
        self.tail_pages[-1].columns[3].write(0) # SCHEMA ENCODING COLUMN
        for index, item in enumerate(new_cols):
            self.tail_pages[-1][index+4].write(item)
        # add this rid to the page directory
        # directory contains dictionary mapping rid to a tuple telling table where to find it
        # tuple contains:
        # ('base' or 'tail', which base/tail page it is found on, the index within that page)
        location = ('tail', len(self.tail_pages)-1, self.tail_pages[-1].num_records-1)
        self.page_directory[rid] = location
        #update indirection columns and schema encoding columns from previous record

    def add_record(self, columns):
        """
        :param columns: list    # List of column values
        """
        if not self.base_pages.columns[0].has_capacity():
            self.base_pages.append(Base_Page(len(columns), self.key))
            self.add_record(columns)
        # first, create a record object from the columns
        rid = self.assign_rid()
        record = Record(self.key, columns, rid)
        # next, add the metadata to columns
        self.base_pages[-1].columns[0].write(0) # INDIRECTION COLUMN
        self.base_pages[-1].columns[1].write(rid) # RID COLUMN
        self.base_pages[-1].columns[2].write(0) # TIMESTAMP COLUMN
        self.base_pages[-1].columns[3].write(0) # SCHEMA ENCODING COLUMN
        for index, item in enumerate(columns):
            self.base_pages[-1][index+4].write(item)
        # finally add this rid to the page directory
        # directory contains dictionary mapping rid to a tuple telling table where to find it
        # tuple contains:
        # ('base' or 'tail', which base/tail page it is found on, the index within that page)
        location = ('base', len(self.base_pages)-1, self.base_pages[-1].num_records-1)
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
