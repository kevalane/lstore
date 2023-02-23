from lstore.table import Table
from lstore.bufferpool import Bufferpool

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
        self.bufferpool = None

    def open(self, path: str, max_pages_in_bufferpool=16) -> None:
        """
        Not required for Milestone 1.
        """
        self.bufferpool = Bufferpool(max_pages_in_bufferpool)
        pass

    def close(self) -> None:
        """
        Not required for Milestone 1.
        """
        pass

    def create_table(self, name: str, num_columns: int, key_index: int) -> Table:
        """
        Creates a new table in the database.

        :param name: str            The name of the table to be created.
        :param num_columns: int     The number of columns in the table.
                                    All columns are of integer type.
        :param key_index: int       The index of the table key in the columns.

        :returns: Table | None      The created table or None if the table already exists.
        """
        if name not in self.tables:
            # Table does not exist, create it
            table = Table(name, num_columns, key_index)
            self.tables[name] = table
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
        return self.tables.get(name, None)