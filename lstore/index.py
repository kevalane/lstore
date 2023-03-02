from lstore import wide_page
from lstore.record import Record
from lstore.page import Page
from lstore.wide_page import Wide_Page
from lstore.config import *

INCLUSIVE = 1 # 0/1 for exclusive/inclusive range

class Index:
    """
    A data strucutre holding indices for various columns of a table. 
    Key column should be indexed by default, other columns can be indexed 
    through this object.
    """

    def __init__(self, table, num_columns=5, key_index=0):
        """
        # Initializes an empty indices dictionary for a table
        :param  table: Table         The table to index
        """
        self.table = table
        self.indices = {}
        self.wide_page = Wide_Page(num_columns, key_index)
    
    
    def locate(self, column: int, value: int) -> list[int]:
        """
        Returns the RIDs of all records with values in "column" equal to "value"

        :param  column: int          The column number to search
        :param  value:  int          The value to search for
        
        :returns: List[int]          Returns an array of RIDs that match the search
        """
        # indicates whether index already made for given column
        if column in self.indices: 
            # gets the index {} associated with the column number
            working_index = self.indices.get(column) 

            # gets the keys associated with the value
            RID_list = working_index.get(value,[])

        else:
            # if index not made, make one
            self.create_index(column)

            # sets the newly created empty index to RID_list
            working_index = self.indices.get(column)
            RID_list = working_index.get(value,[])

        return RID_list 


    def locate_range(self, begin: int, end: int, column: int) -> list[int]:
        """
        Returns the RIDs of all records with values in "column" equal to "value"

        :param column: int - The column number to search
        :param value: int - The value to search for

        :returns: List[int] - An array of RIDs that match the search
        """
        range_RID_list = []

        for i in range(begin,end+INCLUSIVE):
            # append the RID list for each value in the range
            range_RID_list.extend(self.locate(column,i))

        return range_RID_list

    def create_index(self, column_number: int) -> bool:
        """
        # Create index on specific column
        :param  column_number: int      The column number to index

        :returns boolean:               True if index created,
                                        False if index already exists
        """
        if column_number not in self.indices:
            # create index {} for column
            self.indices[column_number] = {}
            self.index_existing_records(column_number)
            # self.push_initialized_records_to_index(self.initialize_index())
            return True
        else:
            return False

    def push_record_to_index(self, record: Record, index_column = None ) -> None:
        """
        # Add a record to all relevant indices
        :param  record: Record       The record to add to the indices
        :param index_column: int     Column for record to be indexed on, defaults to RID if no argument given
        """
        # if index_column == None:
        #     indexed_value = record.rid
        # else:
        #     indexed_value = record.columns[index_column]

        # we should always insert the rid, that's how we find the record
        indexed_value = record.rid

        # iterate through each column in the record
        for i, value in enumerate(record.columns):
            # if index not made for column, skip
            if i not in self.indices:
                continue

            # get all currently indexed values for the column
            working_index = self.indices.get(i)
            if value in working_index:
                # it's already been indexed
                if indexed_value in working_index[value]:
                    pass
                else:
                    # push rid to list of indexed values
                    working_index[value].append(indexed_value)


            if value not in working_index:
                # create a list for the value if it doesn't exist
                working_index[value] = []
                working_index[value].append(indexed_value)


        # print(self.indices)
    def remove_record_from_index(self,record, index_column = None) -> None:
        """
        # Remove a record from all relevant indices
        :param  record: Record       The record to remove from the indices
        :param index_column: int     Column for record to be indexed on, defaults to RID if no argument given
        """
        if index_column == None:
            indexed_value = record.rid
        else:
            indexed_value = record.columns[index_column]
        #RID = record.rid
        #indexed_value = record.columns[index_column]
        for i, value in enumerate(record.columns):
            if self.indices.get(i):
                # get the index {} associated with the column number
                working_index = self.indices.get(i)

                if value in working_index and indexed_value in working_index[value]:
                    # remove the RID from the list if found
                    working_index[value].remove(indexed_value)

    def update_index(self, base_record: Record, tail_record: Record,
                     schema_encoding: str, index_column = None ) -> None:
        """
        # Update index after a record is updated
        :param  base_page: Page      The page containing the old base record
        :param  tail_page: Page      The page containing the new tail record
        :param  schema_encoding: str The schema encoding of updated fields, e.g. 1011
        :param index_column: int     Column for record to be indexed on, defaults to RID if no argument given
        """
        self.remove_record_from_index(base_record)

        # create the combined record
        if index_column == None:
            new_indexed_value = base_record.rid
        else:
            new_indexed_value = base_record.columns[index_column]
        #new_rid = base_record.rid
        #new_indexed_value = base_record.columns[index_column]
        new_key = base_record.key
        new_columns = [0]*len(base_record.columns)

        # if the tail page has a value for a column, use it,
        # otherwise use the base page's value
        for i in range(len(base_record.columns)):
            if schema_encoding[i] == '1':
                new_columns[i] = tail_record.columns[i]
            else:
                new_columns[i] = base_record.columns[i]

        new_record = Record(new_key, new_columns, new_indexed_value)
        self.push_record_to_index(new_record)


    def drop_index(self, column_number: int) -> bool:
        """
        # Drop index of specific column
        :param  column_number: int   The column number to drop index

        :returns: boolean            True if index dropped, False if not
        """
        if column_number in self.indices:
            del self.indices[column_number]
            return True
        else:
            return False
        
    def index_existing_records(self, column_number: int) -> None:
        records = self.table.get_all_records_in_database()
        for record in records:
            self.push_record_to_index(record, column_number)