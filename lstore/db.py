from lstore.table import Table
from lstore.bufferpool import Bufferpool
import os
import json

class Database:
    """
    Class to represent a database that stores all tables and provides methods to
    create and delete tables. The database is stored in a dictionary where the key
    is the table name and the value is the table object.
    """

    def __init__(self) -> None:
        """
        Constructor for the Database class. Initializes the database with an
        empty dictionary.
        """
        self.tables = {}
        self.table_array = []
        self.bufferpool = None
        self.path = './data'

    def open(self, path: str, max_pages_in_bufferpool=16) -> None:
        """
        Not required for Milestone 1.
        """
        if not path.startswith('./'):
            path = './' + path

        try:
            os.mkdir(path)
        except FileExistsError: 
            pass
        self.path = path
        # we need to add path here so disk is stored to specidied folder
        self.bufferpool = Bufferpool(max_pages_in_bufferpool, path)

    def close(self) -> None:
        """
        Not required for Milestone 1.
        """
        for table in self.tables.values():
            table._write_metadata()
        
    def create_table(self, name: str, num_columns: int, key_index: int, new=True) -> Table:
        """
        Creates a new table in the database.

        :param name: str            The name of the table to be created.
        :param num_columns: int     The number of columns in the table.
                                    All columns are of integer type.
        :param key_index: int       The index of the table key in the columns.

        :returns: Table | None      The created table or None if the table already exists.
        """
        self.open(self.path)
        if name not in self.tables:
            # Table does not exist, create it
            print(num_columns, key_index, self.path, new)
            table = Table(name, num_columns, key_index, self.path, new)
            
            self.tables[name] = table
            self.table_array.append(name)
            return table
        else:
            return None

    def drop_table(self, name: str) -> bool:
        """
        Deletes the specified table from the database.

        :param name: str            The name of the table to be deleted.

        :returns: bool              True if the table was deleted, False if the table does not exist.
        """
        if name in self.tables:
            # Table exists, delete it
            del self.tables[name]
            return True
        else:
            return False

    def get_table(self, name: str) -> Table:
        """
        Returns the table with the given name.

        :param name: str            The name of the table to return.

        :returns: Table | None      The table with the given name or None if the table does not exist.
        """
        if name in self.tables:
            return self.tables[name]
        
        try:
            with open(f'{self.path}/{name}/metadata.json', 'r') as f:
                json_data = f.read()
                data = json.loads(json_data, object_hook=jsonKeys2int)
                loaded_table = self.create_table(name, data['num_columns'], data['key'], new=False)
                print(loaded_table)
                loaded_table._load_metadata(data)
                return loaded_table
        except Exception as e:
            print(f'Error loading table {name} from disk')
            return None
        
# @staticmethod
def jsonKeys2int(x):
    # if isinstance(x, dict):
    #     return {int(k):v for k,v in x.items()}
    # return x

    if isinstance(x, dict):
        new_dict = {}
        for k, v in x.items():
            if isinstance(k, str) and k.isnumeric():
                k = int(k)
            new_dict[k] = jsonKeys2int(v)
        return new_dict
    elif isinstance(x, list):
        return [jsonKeys2int(item) for item in x]
    else:
        return x