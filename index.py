"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

# NOTES FROM SEAN
# probably the most important part of this milestone
# also pretty tough to implement cause the given skeleton
# doesn't show a lot of the functionality you need
# super hard if you implement as a binary tree, but the database
# will run much faster if you wanna do it that way
# if you don't wanna do a btree, the easiest way to do this is with dictionaries (definitely slower)
# keep in mind that each table you create will eventually get an index


# below I provide suggestions for the easiest implementation.
# it might be better to implement a different way to get faster runtimes.
# look up ways to implement with btree if you are curious and would like to challenge yourself.
class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        # easiest way to use this indices list is to add a dictionary for each new column where you insert
        self.indices = [None] *  table.num_columns
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # IF YOU ARE IMPLEMENTING WITH DICTIONARIES (if you wanna do btree you gotta do other shit)
        # col = self.indicies[column]
        # if value in col:
        #   return col[value] (return type will be a list)   CASE WHERE VALUE DOES EXIST
        # return [None]   CASE WHERE VALUE DOES NOT EXIST
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # res = []
        # col = self.indices[column]
        # for value from begin to end
        #   if value in col
        #       append col[value] to res
        # return res
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        # this is optional, but i would recommend implementing this cause you kinda need it to make the indexing work
        # if self.indices[column_number] is None
        #   set self.indices[column_number] to an empty dictionary
        #   return
        # return false or some sort of message if the column already has an index
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        # self.indices[column_number] = None
        pass

    # OTHER FUNCTIONS THAT WOULD BE HELPFUL

    # put all the record values in the correct location in self.indices
    def insert_record(self, record, location):
        pass

    # locate all the values of a record in self.indices and remove its rid
    def remove_record(self, record):
        pass
