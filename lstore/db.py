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
    """
    def create_table(self, name, num_columns, key_index):
        table = Table(name, num_columns, key_index)
        self.tables[str(table.name)] = table
        return table

    
    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        if (self.tables[str(name)]):
            return self.tables[str(name)]
        else:
            return -1
