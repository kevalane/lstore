from lstore.page import Page
import json

class Wide_Page:
    """
    :param num_columns: string  # Number of columns in the table
    :param key_index: int       # Index of the key column
     """
    def __init__(self, num_columns: int, key_index: int) -> None:
        self.key_index = key_index
        self.columns = []
        for _ in range(num_columns+4):
            # Add a column for every column being added, plus 4 for the metadata columns
            self.columns.append(Page())

    def write_to_disk(self, index: int, base_page: bool) -> None:
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
                'data': column.data.decode()
            })
        
        if base_page:
            with open(f'./data/base/{index}.json', 'w') as f:
                json.dump(data, f)
        else:
            with open(f'./data/tail/{index}.json', 'w') as f:
                json.dump(data, f)
        
        f.close()

    