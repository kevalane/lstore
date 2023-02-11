from lstore.table import Table

# NOTES FROM SEAN
# easiest class to implement
# realistically should take 20-30 min to implement

class Database():

    def __init__(self):
        # NOTES FROM SEAN
        #   idk if you are allowed to turn this to a dict,
        #   but consider doing that because it makes it faster to create and delete
        self.tables = []
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
        # GOOD WAY TO IMPLEMENT
        # skeleton code already creates table
        # append table to self.tables
        # might want to create logic to make sure every table has a unique name
        #   (also why dictionary would make this faster)
        return table


    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        # GOOD WAY TO IMPLEMENT
        # for table in self.tables:
        #   if table.name == name:
        #       self.table.remove(table) IDK IF THIS LINE ACTUALLY WORKS IM NOT THAT GOOD AT PYTHON
        pass


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        # GOOD WAY TO IMPLEMENT
        # for table in self.tables:
        #   if table.name == name:
        #       return table   CASE WHERE YOU FIND A TABLE WITH THAT NAME
        # return None   CASE WHERE YOU DO NOT FIND A TABLE WITH THAT NAME
        pass
