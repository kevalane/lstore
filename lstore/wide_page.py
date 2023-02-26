from lstore.page import Page
import json
import os

META_COLUMNS = 4

class Wide_Page:
    """
    :param num_columns: string  # Number of columns in the table
    :param key_index: int       # Index of the key column
     """
    def __init__(self, num_columns: int, key_index: int) -> None:
        self.key_index = key_index
        self.columns = []
        for _ in range(num_columns+META_COLUMNS):
            # Add a column for every column being added, plus 4 for the metadata columns
            self.columns.append(Page())

    def write_to_disk(self, index: int, base_page: bool, path='data') -> bool:
        """
        :param index: index to write
        :param base_page: bool to determine if base page or tail page
        :return: None
        """
        data = {
            'key_index': self.key_index,
            'columns': []
        }
        for column in self.columns:
            data['columns'].append({
                'num_records': column.num_records,
                'data': column.data.decode('iso-8859-1')
            })
        
        try:
            if base_page:
                with open(f'./' + path + '/base/{index}.json', 'w+') as f:
                    f.seek(0)
                    f.truncate()
                    json.dump(data, f)
            else:
                with open(f'./' + path + '/tail/{index}.json', 'w+') as f:
                    f.seek(0)
                    f.truncate()
                    json.dump(data, f)
            
            f.close()
            return True
        except:
            print("Error writing to disk")
            return False

    def read_from_disk(self, index: int, is_base_page: bool, path='data') -> bool:
        try:
            if is_base_page:
                with open(f'./' + path + '/base/{index}.json', 'r') as f:
                    data = json.load(f)
            else:
                with open(f'./' + path + '/tail/{index}.json', 'r') as f:
                    data = json.load(f)
        except:
            print("Error reading from disk")
            return False

        self.key_index = data['key_index']
        for i, column in enumerate(data['columns']):
            self.columns[i].num_records = column['num_records']
            self.columns[i].data = bytearray(column['data'].encode('iso-8859-1'))
        
        f.close()
        return True

    