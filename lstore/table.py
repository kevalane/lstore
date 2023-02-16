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
    def __init__(self, num_columns: int, key_index: int) -> None:
        self.key_index = key_index
        self.columns = []
        for _ in range(num_columns+4):
            # Add a column for every column being added, plus 4 for the metadata columns
            self.columns.append(Page())


class Tail_Page:

    def __init__(self, num_columns: int, key_index: int) -> None:
        key_index = key_index
        self.columns = []
        for _ in range(num_columns+4):
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
    def __init__(self, name: str, num_columns: int, key_index: int) -> None:
        self.name = name
        self.key = key_index
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.index.create_index(key_index)
        self.base_pages = [Base_Page(num_columns, key_index)]
        self.tail_pages = []
        # Keeps track of the RID to be generated each time a tail record is added
        self.rid_generator = 0 

    def get_record(self, rid: int, with_meta=False) -> list[int]:
        """
        :param rid: int:        # RID of record to be retrieved
        """
        # Get the relevant page and information from the page directory.
        page_type, page_num, offset = self.page_directory[rid]
        page = self.base_pages if page_type == 'base' else self.tail_pages
        
        # check if updated
        update = page[page_num].columns[SCHEMA_ENCODING_COLUMN].get(offset)
        update_str = str(update)
        # append 0 to start if shorter than num_columns
        update_str = '0'*(self.num_columns-len(update_str)) + update_str
        vals = []

        for column in range(len(page[page_num].columns)):
            vals.append(page[page_num].columns[column].get(offset))

        if not (update == 0 or type(page) == Tail_Page):
            # we're only here if base page that's updated
            latest_tail_rid = page[page_num].columns[INDIRECTION_COLUMN].get(offset)
            vals = self.get_tail_page(latest_tail_rid)

            for i in range(len(vals[META_COLUMNS:])):
                if (update_str[i] == '0'):
                    vals[i+META_COLUMNS] = page[page_num].columns[META_COLUMNS+i].get(offset)
        
        # return the wanted values
        if with_meta:
            return vals
        else:
            return vals[META_COLUMNS:] #return values except the first 4 metadata columns

    def get_tail_page(self, tail_rid):
        """
        :param tail_rid: int     # RID of the tail page to be retrieved
        """
        page_num = self.page_directory[tail_rid][PAGE_NUM]
        page_offset = self.page_directory[tail_rid][OFFSET]
        retvals = []
        for column in range(len(self.tail_pages[page_num].columns)):
            retvals.append(self.tail_pages[page_num].columns[column].get(page_offset))
        
        return retvals

    def get_base_record(self, base_rid):
        """
        :param base_rid: int     # RID of the base page to be retrieved
        """
        page_num = self.page_directory[base_rid][PAGE_NUM]
        page_offset = self.page_directory[base_rid][OFFSET]
        retvals = []
        for column in range(len(self.base_pages[page_num].columns)):
            retvals.append(self.base_pages[page_num].columns[column].get(page_offset))
        
        return retvals

    def delete_record(self, rid):
        """
        :param rid: int         # rid to be deleted
        """
        if rid not in self.page_directory:
            return False

        indir_rid = self.get_base_record(rid)[INDIRECTION_COLUMN]

        if indir_rid != 0:
            checking = indir_rid
            while checking != rid:
                self.page_directory[checking*-1] = self.page_directory[checking]
                new_checking = self.get_tail_page(checking)[INDIRECTION_COLUMN]
                del self.page_directory[checking]
                checking = new_checking

        self.page_directory[rid*-1] = self.page_directory[rid]
        del self.page_directory[rid]
                
        return True
        
        

    def get_column(self, column):
        """
        :param column: int      # Index of column to be retrieved
        """
        col_list = []
        for rid in self.page_directory.keys():
            if self.page_directory[rid][0] == 'base' and rid >= 0:
                val = (self.get_record(rid)[self.key], self.get_record(rid)[column])
                col_list.append(val)
        return col_list

        

    def update_record(self, rid, new_cols):
        """
        :param new_cols: list   # List of new column values
        :param rid: int         # RID of the previous record being updated
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
        tail_record = Record(self.key, new_cols, tail_rid)

        # insert the specified values in the tail page columns
        for index, item in enumerate(new_cols):
            if (item == None):
                item = 0
            self.tail_pages[-1].columns[index+META_COLUMNS].write(item)
        
        # add this rid to the page directory
        # directory contains dictionary mapping rid to a tuple telling table where to find it
        # tuple contains:
        # ('base' or 'tail', which base/tail page it is found on, the index within that page)
        location = ('tail', len(self.tail_pages)-1, 
                    self.tail_pages[-1].columns[INDIRECTION_COLUMN].num_records)
        
        self.page_directory[tail_rid] = location
        #update indirection columns and schema encoding columns from previous record

        # tail_rid must be written to indir column of base page,
        base_record = self.page_directory[rid]
        base_page = self.base_pages[base_record[PAGE_NUM]]
        old_tail_rid = base_page.columns[INDIRECTION_COLUMN].get(base_record[OFFSET])
        base_page.columns[INDIRECTION_COLUMN].put(tail_rid, base_record[OFFSET])

        # add metadata to columns
        self.tail_pages[-1].columns[INDIRECTION_COLUMN].write(old_tail_rid)
        if (tail_rid != rid):
            self.tail_pages[-1].columns[RID_COLUMN].write(tail_rid)
        self.tail_pages[-1].columns[TIMESTAMP_COLUMN].write(0)

        # HANDLE CUMULATIVE SCHEMA UPDATES
        previous_encoding = 0
        if (old_tail_rid != rid):
            old_tail_offset = self.page_directory[old_tail_rid][OFFSET]
            old_tail_info = self.get_tail_page(old_tail_rid)
            old_tail_encoding = old_tail_info[SCHEMA_ENCODING_COLUMN]
            previous_encoding = old_tail_encoding

            # write all old info to new tail page
            for i in range(len(old_tail_info[META_COLUMNS:])):
                if (old_tail_info[i+META_COLUMNS] != 0 and self.tail_pages[-1].columns[i+META_COLUMNS].get(location[OFFSET]) == 0):
                    self.tail_pages[-1].columns[i+META_COLUMNS].put(old_tail_info[i+META_COLUMNS], location[OFFSET])
        
        previous_encoding = '0'*(self.num_columns - len(str(previous_encoding))) + str(previous_encoding)

        # create update schema column (1 if updated, 0 if not)
        encoding = '0'*self.num_columns
        for i in range(len(new_cols)):
            if (new_cols[i] != None or previous_encoding[i] == '1'):
                encoding = encoding[:i] + '1' + encoding[i + 1:]

        self.tail_pages[-1].columns[SCHEMA_ENCODING_COLUMN].write(int(encoding))
        base_page.columns[SCHEMA_ENCODING_COLUMN].put(int(encoding), base_record[OFFSET])

        # create base record
        base_record_cols = []
        for i in range(len(base_page.columns[META_COLUMNS:])):
            base_record_cols.append(base_page.columns[i].get(base_record[OFFSET]))
        base_rec = Record(self.key, base_record_cols, rid)

        # pad with leading 0s
        encoding = '0'*(self.num_columns - len(str(encoding))) + str(encoding)

        # update indexing
        self.index.update_index(base_rec, tail_record, str(encoding))


    def add_record(self, columns):
        """
        :param columns: list    # List of column values
        """
        # check if there is capacity in the last base page
        if not self.base_pages[-1].columns[0].has_capacity():
            self.base_pages.append(Base_Page(len(columns), self.key))
            return self.add_record(columns)

        # first, create a record object from the columns
        # rid = self.assign_rid()
        rid = columns[0] # rid is given by the user
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

    def _pad_with_leading_zeros(self, encoding: int) -> str:
        """
        Pads the encoding with leading zeros to make it the 
        same length as the number of columns
        :param encoding: int    # The encoding to pad
        
        :return: str            # The padded encoding
        """
        return '0'*(self.num_columns - len(str(encoding))) + str(encoding)

    '''
    MERGE WILL BE IMPLEMENTED IN MILESTONE 2
    def __merge__(self):
        print("merge is happening")
        pass
    '''
