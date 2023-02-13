"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = {}
        pass

    
    def locate(self, column, value):
        #given a value and column number, go through the corresponding index dictionary and 
        #find the value 
        #if you find the value, return the corresponding key list
        #else, return an empty list
        if column in self.indices: #indicates whether index already made for given column
            working_index = self.indices.get(column) #gets the index associated with the column number
            RID_list = working_index.get(value,[]) #gets the keys associated with the value

        else:
            self.create_index(column)
            working_index = self.indices.get(column) #gets the index associated with the column number
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
    def create_index(self, column_number):
        #for column number, create an empty dictionary. go through records,
        #store column value as value, RID column as key list. loop through each record
        #and append to key list if value already exists, else create new dictionary entry
        if column_number not in self.indices:
            self.indices[column_number] = {}
            return True
        else:
            return False

    def push_record_to_index(self,record):
        RID = record.rid 
        for i, value in enumerate(record.columns):
            self.create_index(i)
            working_index = self.indices.get(i)
            if value in working_index:
                rid_flag = 0
                for current_RID in working_index.values():
                    if current_RID == RID:
                        rid_flag = 1
                        break
                if rid_flag == 0:
                    working_index[value].append(RID)
            else:
                working_index[value] = [RID]

    def remove_record_from_index(self,record):
        RID = record.rid
        for i, value in enumerate(record.columns):
            if self.indices.get(i) == False:
                continue
            else:
                working_index = self.indices.get(i)
                if value in working_index and RID in working_index[value]:
                    working_index[value].remove(RID)
            

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
