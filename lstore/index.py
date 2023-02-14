INCLUSIVE = 1 # 0/1 for exclusive/inclusive range

"""
A data strucutre holding indices for various columns of a table. 
Key column should be indexed by default, other columns can be indexed 
through this object.
"""
class Index:

    """
    # Initializes an empty indices dictionary for a table
    :param  table: Table         The table to index
    """
    def __init__(self, table):
        self.indices = {}
    
    """
    # Returns the RIDs of all records with values in "column" equal to "value"
    :param  column: int          The column number to search
    :param  value:  int          The value to search for
    
    @returns RID_list: int[]     Returns an array of RIDs that match the search
    """
    def locate(self, column: int, value: int) -> list[int]:
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


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    :param  begin:  int          The beginning of the range
    :param  end:    int          The end of the range
    :param  column: int          The column number to search

    @returns RID_list: int[]     Returns an array of RIDs that match the search
    """
    def locate_range(self, begin: int, end: int, column: int) -> list[int]:
        range_RID_list = []

        for i in range(begin,end+INCLUSIVE):
            # append the RID list for each value in the range
            range_RID_list.extend(self.locate(column,i))

        return range_RID_list

    """
    # Create index on specific column
    :param  column_number: int   The column number to index

    @returns boolean             True if index created,
                                 False if index already exists
    """
    def create_index(self, column_number: int) -> bool:
        if column_number not in self.indices:
            # create index {} for column
            self.indices[column_number] = {}
            return True
        else:
            return False

    """
    # Add a record to all relevant indices
    :param  record: Record       The record to add to the indices
    """
    def push_record_to_index(self, record) -> None:
        RID = record.rid
        # iterate through each column in the record
        for i, value in enumerate(record.columns):
            # if index not made, make one
            self.create_index(i)

            # get the index {} associated with the column number
            working_index = self.indices.get(i)

            if value not in working_index:
                # create a list for the value if it doesn't exist
                working_index[value] = []

            if RID not in working_index[value]:
                # add the RID to the list if it's not already there
                working_index[value].append(RID)


    def remove_record_from_index(self,record):
        RID = record.rid
        for i, value in enumerate(record.columns):
            if self.indices.get(i):
                working_index = self.indices.get(i)
                if value in working_index and RID in working_index[value]:
                    working_index[value].remove(RID)
            
    def update_index(self, base_page, tail_page) -> None:
        self.remove_record_from_index(base_page)
        
        # create the combined record
        new_rid = base_page.rid
        new_key = base_page.key
        new_columns = []

        # if the tail page has a value for a column, use it, 
        # otherwise use the base page's value
        for i in enumerate(base_page.columns):
            if tail_page.columns[i] != None:
                new_columns.append(tail_page.columns[i])
            else:
                new_columns.append(base_page.columns[i])

        # new_record = Record(new_rid, new_key, new_columns)
        # self.push_record_to_index(new_record)

    """
    # optional: Drop index of specific column
    """
    def drop_index(self, column_number):
        if column_number in self.indices:
            del self.indices[column_number]
            return True
        else:
            return False


    
