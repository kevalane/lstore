from lstore.table import Table, Record
from lstore.index import Index

# this is the most user-facing part of the project so, a lot of these functions
# rely on calling the other functions, especially from Table class
# make sure to test all your other classes before working on this class, since it
# is sort of the last layer of abstraction for people to use this function
# this is also why I don't recommend splitting the work by class, since some files rely a lot on
# other files to function properly

# if you are planning on coding separately, make sure at least one person in the group
# is a Github god otherwise versioning and getting all the files to function together is going to be a hassle
# pretty sure there's also hidden test cases, so it is probably best to add more functionality than you need
# (helps with getting extra credit too if you go above and beyond the prompt)

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass


    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        # call self.table.delete_record
        # will need error checking, using try block helps with this (also calling self.table.get_record)
        # also try to include messages to let user know if record is successfully deleted or not
        pass


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # schema encoding marks which columns have been updated
        schema_encoding = '0' * self.table.num_columns

        # create a list for the columns parameter
        # call self.table.insert_record
        # will need error checking for if number of columns matches number of columns in the table
        # also error checking with a try-except helps to see if the insertion was successful
        pass


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        # make a res output list []
        # call self.table.get_multiple_records using search_key and search_key_index
        # loop thru output of get_multiple records
        #   initialize queried columns list []
        #   loop thru projected_columns_index
        #       if value is 1, then append the corresponding value to the queried columns list
        #   create a new record object with the queried columns list as the columns parameter
        #   append record to res
        # return res
        pass


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        # did not have to do this for my implementation, so idk. dont think it should be too hard tho.
        # might need to make an additionaly function in table.py to help with this
        pass


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # create a list for the columns parameter
        # call self.table.update_record using the primary key and the columns
        # use try-except to check if update succeeded
        pass


    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        # initialize sum variable to 0
        # call self.table.sum with the proper parameters and set sum equal to the return value
        # use try-except to check if sum succeeded
        pass


    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        # again didn't have to implement this before, so look and see if you need to add more table functions
        pass


    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        # probably don't need to touch this, more of a tester to see if the other functions work
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    # MAKE SURE TO INCLUDE A LOCAL MAIN TO TEST IF YOUR FUNCTIONS WORK
