from lstore.index import Index
from lstore.page import Page
from lstore.record import Record
from lstore.bufferpool import Bufferpool
from lstore.wide_page import Wide_Page
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
    def __init__(self, name: str, num_columns: int, key_index: int, path='data') -> None:
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

        # keep track of latest base page
        self.latest_base_page_index = 0
        self.latest_tail_page_index = -1
        self.bufferpool = Bufferpool(10, path)

        # create a base page
        last_base_page = Wide_Page(num_columns, key_index)
        last_base_page.write_to_disk(0, True, path)

    def get_record(self, rid: int, with_meta=False) -> list[int]:
        """
        Get a record from the table with the given rid
        :param rid: int:        # RID of record to be retrieved
        :param with_meta: bool  # Whether or not to include the metadata columns

        :return: list[int]      # List of values in the record
        """
        # Get the relevant page and information from the page directory.
        page_type, page_num, offset = self.page_directory[rid]

        # retrieve the page
        page = self.bufferpool.retrieve_page(page_num, (page_type == 'base'), self.num_columns)
        # page = self.base_pages if page_type == 'base' else self.tail_pages
        
        # check if updated
        update = page.columns[SCHEMA_ENCODING_COLUMN].get(offset)
        update_str = self._pad_with_leading_zeros(update)

        # adds all base page values to vals
        vals = []
        for column in range(len(page.columns)):
            vals.append(page.columns[column].get(offset))

        # NEEDS TO TAKE INTO ACCOUNT IF THE TAIL PAGE IS UPDATED
        # TODO
        if not (update == 0 or type(page) == Tail_Page):
            # we're only here if base page that's updated
            latest_tail_rid = page.columns[INDIRECTION_COLUMN].get(offset)
            tail_vals = self.get_tail_page(latest_tail_rid)

            for i in range(len(vals[META_COLUMNS:])):
                if (update_str[i] == '1'):
                    # if the column is updated, replace the value with the tail value
                    vals[i+META_COLUMNS] = tail_vals[i+META_COLUMNS]
        
        # Return the wanted values
        return vals if with_meta else vals[META_COLUMNS:]

    def get_tail_page(self, tail_rid: int) -> list[int]:
        """
        :param tail_rid: int     # RID of the tail page to be retrieved
        """
        page_num = self.page_directory[tail_rid][PAGE_NUM]
        page_offset = self.page_directory[tail_rid][OFFSET]
        retvals = []
        tail_page = self.bufferpool.retrieve_page(page_num, False, self.num_columns)
        for column in range(len(tail_page.columns)):
            retvals.append(tail_page.columns[column].get(page_offset))
        
        return retvals

    def get_base_record(self, base_rid: int) -> list[int]:
        """
        :param base_rid: int     # RID of the base page to be retrieved
        """
        page_num = self.page_directory[base_rid][PAGE_NUM]
        page_offset = self.page_directory[base_rid][OFFSET]
        retvals = []
        base_page = self.bufferpool.retrieve_page(page_num, True, self.num_columns)
        for column in range(len(base_page.columns)):
            retvals.append(base_page.columns[column].get(page_offset))
        
        return retvals

    def delete_record(self, rid: int) -> bool:
        """
        Delete a record from the table with the given rid
        :param rid: int         # rid to be deleted
        :return: bool           # True if record was deleted, False if not
        """
        if rid not in self.page_directory:
            return False
        
        # get the indirection rid of base record
        indir_rid = self.get_base_record(rid)[INDIRECTION_COLUMN]

        if indir_rid != rid or indir_rid != 0:
            # means its been updated, so we need to invalidate all tail pages
            checking = indir_rid
            while checking != rid:
                # invalidate tail page
                self.page_directory[checking*-1] = self.page_directory[checking]

                # get next tail page
                new_checking = self.get_tail_page(checking)[INDIRECTION_COLUMN]

                # delete old tail page from page directory
                del self.page_directory[checking]
                checking = new_checking

        self.page_directory[rid*-1] = self.page_directory[rid]
        del self.page_directory[rid]
                
        return True

    def get_column(self, column: int) -> list[tuple[int, int]]:
        """
        Get a column from the table with the given column index
        :param column: int      # Index of column to be retrieved
        :return: list[tuple]    # List of tuples of (key, value) for each record
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

        # if no tail pages exist, create one
        if (self.latest_tail_page_index == -1):
            self.latest_tail_page_index += 1
            new_last_tail_page = Wide_Page(self.num_columns, self.key)
            new_last_tail_page.write_to_disk(self.latest_tail_page_index, False)
            return self.update_record(rid, new_cols)
        
        # get the latest tail page
        last_tail_page = self.bufferpool.retrieve_page(
            self.latest_tail_page_index, 
            False, 
            self.num_columns
        )

        # check if there's capacity in last tail_page, recursive if not 
        if not last_tail_page.columns[INDIRECTION_COLUMN].has_capacity():
            self.latest_tail_page_index += 1
            new_last_tail_page = Wide_Page(self.num_columns, self.key)
            new_last_tail_page.write_to_disk(self.latest_tail_page_index, False)
            return self.update_record(rid, new_cols)

        # create a record object
        tail_rid = self.assign_rid()
        tail_record = Record(self.key, new_cols, tail_rid)

        # insert the specified values in the tail page columns
        for index, item in enumerate(new_cols):
            if item is None:
                item = 0
            last_tail_page.columns[index+META_COLUMNS].write(item)
        
        # add rid to page directory
        location = ('tail', self.latest_tail_page_index, 
                    last_tail_page.columns[INDIRECTION_COLUMN].num_records)
        
        self.page_directory[tail_rid] = location

        # tail_rid must be written to indir column of base page,
        base_record = self.page_directory[rid]
        # base_page = self.base_pages[base_record[PAGE_NUM]]
        base_page = self.bufferpool.retrieve_page(base_record[PAGE_NUM], True, self.num_columns)
        old_tail_rid = base_page.columns[INDIRECTION_COLUMN].get(base_record[OFFSET])
        
        # write new tail_rid to base page
        base_page.columns[INDIRECTION_COLUMN].put(tail_rid, base_record[OFFSET])

        # add metadata to columns
        last_tail_page.columns[INDIRECTION_COLUMN].write(old_tail_rid)
        if (tail_rid != rid):
            last_tail_page.columns[RID_COLUMN].write(tail_rid)
        last_tail_page.columns[TIMESTAMP_COLUMN].write(0)

        # HANDLE CUMULATIVE SCHEMA UPDATES
        previous_encoding = 0
        if (old_tail_rid != rid):
            old_tail_info = self.get_tail_page(old_tail_rid)
            old_tail_encoding = old_tail_info[SCHEMA_ENCODING_COLUMN]
            previous_encoding = old_tail_encoding

            # write all old info to new tail page
            for i in range(len(old_tail_info[META_COLUMNS:])):
                # if (old_tail_info[i+META_COLUMNS] != 0 and last_tail_page.columns[i+META_COLUMNS].get(location[OFFSET]) == 0):
                if (old_tail_info[i+META_COLUMNS] != 0 and last_tail_page.columns[i+META_COLUMNS].get(location[OFFSET]) == 0
                    and new_cols[i] == None):
                    last_tail_page.columns[i+META_COLUMNS].put(old_tail_info[i+META_COLUMNS], location[OFFSET])
        
        previous_encoding = self._pad_with_leading_zeros(previous_encoding)

        # create update schema column (1 if updated, 0 if not)
        encoding = '0'*self.num_columns
        for i in range(len(new_cols)):
            if (new_cols[i] != None or previous_encoding[i] == '1'):
                encoding = encoding[:i] + '1' + encoding[i + 1:]

        last_tail_page.columns[SCHEMA_ENCODING_COLUMN].write(int(encoding))
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
        base_page.write_to_disk(base_record[PAGE_NUM], True)
        last_tail_page.write_to_disk(self.latest_tail_page_index, False)


    def add_record(self, columns: list[int]) -> None:
        """
        :param columns: list    # List of column values
        """
        # get last base page
        last_base_page = self.bufferpool.retrieve_page(
            self.latest_base_page_index,
            True,
            self.num_columns
        )
        
        # check if there is capacity in the last base page
        if not last_base_page.columns[0].has_capacity():
            # if not, create a new base page
            new_last_base_page = Wide_Page(self.num_columns, self.key)
            self.latest_base_page_index += 1

            # write to disk
            new_last_base_page.write_to_disk(self.latest_base_page_index, True)

            # recursive call tries to add record again, now that there is capacity
            return self.add_record(columns)

        # create record object from columns
        rid = columns[0] 
        record = Record(self.key, columns, rid)
        
        # add record to index
        self.index.push_record_to_index(record)

        # next, add the metadata to columns
        base_page = last_base_page
        base_page.columns[INDIRECTION_COLUMN].write(rid)
        base_page.columns[RID_COLUMN].write(rid)
        base_page.columns[TIMESTAMP_COLUMN].write(0)
        base_page.columns[SCHEMA_ENCODING_COLUMN].write(0)

        # write to columns
        for index, item in enumerate(columns):
            base_page.columns[index+4].write(item)
        
        # add this rid to the page directory
        location = ('base', 
                    self.latest_base_page_index, 
                    base_page.columns[0].num_records-1)

        self.page_directory[rid] = location
        last_base_page.write_to_disk(self.latest_base_page_index, True)

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
