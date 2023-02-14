from lstore.index import Index
from lstore.page import Page
from lstore.record import Record
from time import time

# page access indexes
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
META_COLUMNS = 4

# page directory indexes
PAGE_TYPE = 0
PAGE_NUM = 1
OFFSET = 2

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

    def get_record(self, rid, with_meta=False):
        """
        :param rid: int:        # RID of record to be retrieved
        """
        # get relevant page depending on tuple value
        if self.page_directory[rid][PAGE_TYPE] == 'base':
            page = self.base_pages
        else:
            page = self.tail_pages
        
        # get relevant info from page_dir
        page_num = self.page_directory[rid][PAGE_NUM]
        offset = self.page_directory[rid][OFFSET]
        
        # check if updated
        update = page[page_num].columns[SCHEMA_ENCODING_COLUMN].get(offset) 
        update_str = str(update)
        vals = []

        if update == 0 or type(page) == Tail_Page:
            # just get base page values
            for column in range(len(page[page_num].columns)):
                vals.append(page[page_num].columns[column].get(offset))
        else:
            # we're only here if base page that's updated
            latest_tail_rid = page[page_num].columns[INDIRECTION_COLUMN].get(offset)
            vals = self.get_record(latest_tail_rid, with_meta=True)
            for(column, val) in enumerate(vals):
                if (update_str[column] == '0'):
                    vals[column] = page[page_num].columns[column].get(offset)
                else:
                    vals[column] = val

        # return the wanted values
        if with_meta:
            return vals
        else:
            return vals[META_COLUMNS:] #return values except the first 4 metadata columns

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
        # create tail page if none exist
        if self.tail_pages == []:
            self.tail_pages.append(Tail_Page(len(new_cols), self.key))

        # check if there's capacity in last tail_page, recursive if not 
        if not self.tail_pages[-1].columns[INDIRECTION_COLUMN].has_capacity():
            self.tail_pages.append(Tail_Page(len(new_cols), self.key))
            return self.update_record(rid, new_cols)

        # create a record object
        tail_rid = self.assign_rid()
        record = Record(self.key, new_cols, tail_rid)

        # insert the specified values in the tail page columns
        for index, item in enumerate(new_cols):
            self.tail_pages[-1].columns[index+4].write(item)
        
        # add this rid to the page directory
        # directory contains dictionary mapping rid to a tuple telling table where to find it
        # tuple contains:
        # ('base' or 'tail', which base/tail page it is found on, the index within that page)
        location = ('tail', len(self.tail_pages)-1, 
                    self.tail_pages[-1].columns[INDIRECTION_COLUMN].num_records-1)
        
        self.page_directory[tail_rid] = location
        #update indirection columns and schema encoding columns from previous record

        # tail_rid must be written to indir column of base page,
        base_record = self.page_directory[rid]
        base_page = self.base_pages[base_record[PAGE_NUM]]
        old_tail_rid = base_page.columns[INDIRECTION_COLUMN].get(base_record[OFFSET])
        base_page.columns[INDIRECTION_COLUMN].put(base_record[OFFSET], tail_rid)

        # add metadata to columns
        self.tail_pages[-1].columns[INDIRECTION_COLUMN].write(old_tail_rid)
        self.tail_pages[-1].columns[RID_COLUMN].write(tail_rid)
        self.tail_pages[-1].columns[TIMESTAMP_COLUMN].write(0)

        # create update schema column (1 if updated, 0 if not)
        encoding = 0
        for i in range(len(new_cols)):
            if (new_cols[i] != None):
                encoding += 1
                if (i != len(new_cols)-1):
                    encoding *= 10

        self.tail_pages[-1].columns[SCHEMA_ENCODING_COLUMN].write(encoding)
        base_page.columns[SCHEMA_ENCODING_COLUMN].put(base_record[OFFSET], encoding)


    def add_record(self, columns):
        """
        :param columns: list    # List of column values
        """
        # check if there is capacity in the last base page
        if not self.base_pages[-1].columns[0].has_capacity():
            self.base_pages.append(Base_Page(len(columns), self.key))
            return self.add_record(columns)
        # first, create a record object from the columns
        rid = self.assign_rid()
        record = Record(self.key, columns, rid)
        self.index.push_record_to_index(record)
        # next, add the metadata to columns
        self.base_pages[-1].columns[INDIRECTION_COLUMN].write(rid) # INDIRECTION COLUMN
        self.base_pages[-1].columns[RID_COLUMN].write(rid) # RID COLUMN
        self.base_pages[-1].columns[TIMESTAMP_COLUMN].write(0) # TIMESTAMP COLUMN
        self.base_pages[-1].columns[SCHEMA_ENCODING_COLUMN].write(0) # SCHEMA ENCODING COLUMN
        for index, item in enumerate(columns):
            self.base_pages[-1].columns[index+4].write(item)
        # finally add this rid to the page directory
        # directory contains dictionary mapping rid to a tuple telling table where to find it
        # tuple contains:
        # ('base' or 'tail', which base/tail page it is found on, the index within that page)
        location = ('base', 
                    len(self.base_pages)-1, 
                    self.base_pages[-1].columns[0].num_records-1)
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
