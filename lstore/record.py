class Record:

    def __init__(self, key, columns, rid=None):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __getitem__(self, index):
        return self.columns[index]

    def __get__(self):
        return self.columns