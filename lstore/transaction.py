from imp import acquire_lock
from lstore.table import Table, Record
from lstore.index import Index
from lstore.query import Query
from lstore.lock import Lock
from lstore.config import *

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.tables = []
        self.success = 0
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, table, *args):
        self.queries.append((query, args))
        self.tables.append(table)
        # use grades_table for aborting

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            
            result = query(*args)

            # Implementing 2PL for update and delete
            # if 'Query.delete' in str(query):
            #     args[0].acquire_rid()
            # elif 'Query.update' in str(query):
            #     args[0].acquire_index()

            # If the query has failed the transaction should abort
            
            if result == False:
                return self.abort()
            
            self.success += 1
            
        return self.commit()

    
    def abort(self):
        count = 0
        
        for query, args in self.queries:
            if self.success == count:
                return False
            
            if 'Query.delete' in str(query):
                try:
                    self.tables[count].delete_record(-args[0])
                    # Releasing locks as transactions are aborted
                    #args[0].release_rid()
                except:
                    continue
                
            elif 'Query.insert' in str(query):
                try:
                    self.tables[count].delete_record(args[0])
                except:
                    continue
                
            elif 'Query.update' in str(query):
                try:
                    page_type, page_num, offset = self.tables[count].page_directory[args[0]]
                    page = self.tables[count].bufferpool.retrieve_page(page_num, (page_type == 'base'), self.tables[count].num_columns)
                    
                    latest_tail_rid = page.columns[INDIRECTION_COLUMN].get(offset)
                    page_type, t_page_num, t_offset = self.tables[count].page_directory[latest_tail_rid]
                    
                    tail_page = self.tables[count].bufferpool.retrieve_page(t_page_num, (page_type == 'base'), self.tables[count].num_columns)
                    
                    page.columns[INDIRECTION_COLUMN].put(tail_page.columns[INDIRECTION_COLUMN].get(offset), offset)
                    
                    self.tables[count].bufferpool.base_pages[page_num]['wide_page'] = page
                    # Releasing locks as transactions are aborted
                    # args[0].release_index()
                except:
                    continue
                
            count += 1
        return False

    
    def commit(self):
        # Releasing locks as a part of the commit process
        # self[0].release_index()
        # self[0].release_rid()
        return True

