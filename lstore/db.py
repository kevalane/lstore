from lstore.table import Table

class Database():

    def __init__(self):
        self.tables = {} # hashtable for O(1) delete / get
        pass

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    
    @returns Table | None       Returns the created table 
                                or None if table already exists
    """
    def create_table(self, name, num_columns, key_index):
        if (self.get_table(name) == None):
            # Table does not exist, create it
            table = Table(name, num_columns, key_index)
            self.tables[str(table.name)] = table
            return table
        else:
            return None

    
    """
    # Deletes the specified table

    :param name: string         #Table name

    @returns boolean            Returns True if table was deleted,
                                False if table does not exist
    """
    def drop_table(self, name):
        if (self.get_table(name) != None):
            # Table exists, delete it
            del self.tables[str(name)]
            return True
        else:
            return False

    
    """
    # Returns table with the passed name

    :param name: string         #Table name

    @returns Table | None       Returns the table with given name 
                                or None if table does not exist
    """
    def get_table(self, name):
        if name in self.tables:
            return self.tables[str(name)]
        else:
            return None
