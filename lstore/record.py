class Record:

    def __init__(self, key, columns, rid=None):
        self.rid = rid
        self.key = key
        self.columns = columns