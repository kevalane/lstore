"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass

    
    def locate(self, column, value):
        #given a value and column number, go through the corresponding index dictionary and 
        #find the value 
        #if you find the value, return the corresponding key list
        #else, return an empty list
        if column in indices: #indicates whether index already made for given column
            working_index = indices.get(column) #gets the index associated with the column number
            RID_list = working_index.get(value,[]) #gets the keys associated with the value

        else:
            self.create_index(column)
            working_index = indices.get(column) #gets the index associated with the column number
            RID_list = working_index.get(value,[]) #gets the keys associated with the value

        return RID_list 


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        range_RID_list = []
        for i in range[begin,end]:
            range_RID_list.append(self.locate(column,i))

        return range_RID_list
        #given a range of values and a column number, go through the corresponding column index
        #dict and serach for the range of values
        #initiate an empyty list in the beginning to store all the key lists per value, append
        #after each value is searched for 
        
        pass

    """
    # optional: Create index on specific column
    """
indices = {}

    def create_index(self, column_number):
        #for column number, create an empty dictionary. go through records, 
        #store column value as value, RID column as key list. loop through each record 
        #and append to key list if value already exists, else create new dictionary entry
        index = {}
        for record in Table:
            value = Table[column_number]
            RID = Table[0]
            if value in index:
                index[value].append(RID)
            else:
                index[value] = [RID]
        indices[column_number] = index
        
        return indices

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
