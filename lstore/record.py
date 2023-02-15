class Record:

    def __init__(self, key: int, columns: list[int], rid=None):
        """
        Constructor for the Record class.
        
        :param  key:        int             The key for the record
        :param  columns:    List[int]       The columns of the record
        :param  rid:        int, optional   The record ID, defaults to None
        """
        self.rid = rid
        self.key = key
        self.columns = columns

    def __getitem__(self, index: int) -> int:
        """
        Returns the column value at the given index.
        
        :param  index:      int             The index of the column to retrieve
        :returns:           int             The value of the column at the given index
        """
        return self.columns[index]

    def __get__(self) -> list[int]:
        """
        Returns the columns of the record.
        
        :returns:           List[int]       The columns of the record
        """
        return self.columns

    def __str__(self) -> str:
        """
        Returns a string representation of the record.
        
        :returns:           str             A string representation of the record
        """
        return str(self.columns)
