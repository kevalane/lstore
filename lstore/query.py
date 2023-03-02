from lstore.table import Table
from lstore.index import Index
from lstore.record import Record
from lstore.config import *


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            retval = self.table.delete_record(primary_key)
            return retval

        except: 
            return False
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        if len(columns) != self.table.num_columns:
            return False
        
        try:
            if not self.table.add_record(columns):
                return False
            return True
            
        except Exception as e:
            print(e)
            return False
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        res = list()
        
        try:
            if (len(projected_columns_index) > self.table.num_columns):
                return False

            try:
                selected = self.table.index.indices[search_key_index].get(search_key)
            except:
                selected = self.table.index.indices[str(search_key_index)].get(str(search_key))
            for rid in selected:
                record = self.table.get_record(rid)
                cols = list()
                for i in range(len(projected_columns_index)):
                    if projected_columns_index[i] == 1:
                        cols.append(record[i])
                
                res.append(Record(self.table.key, cols, rid))
            return res
        
        except Exception as e:
            res = list()
            # Using indices did not work
            records = self.table.brute_force_search(search_key, search_key_index)
            for record in records:
                cols = list()
                for i in range(len(projected_columns_index)):
                    if projected_columns_index[i] == 1:
                        cols.append(record[i])
                res.append(Record(self.table.key, cols, record[1]))
            return res
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        res = list()
        
        try:
            if (len(projected_columns_index) > self.table.num_columns):
                return False
            
            selected = self.table.index.indices[search_key_index].get(search_key)
        
            for rid in selected:
                record = self.table.get_base_record(rid)
                base_record = record
                
                for j in range(abs(relative_version)+1):
                    # how many times to go backwards
                    indirection_tid = record[0]
                    if (indirection_tid != rid):
                        tail = self.table.get_tail_page(indirection_tid)
                        record = tail
                    else:
                        record = self.table.get_base_record(indirection_tid)
                        break
                
                schema_encoding = record[SCHEMA_ENCODING_COLUMN]
                schema_encoding = self.table._pad_with_leading_zeros(schema_encoding)
                for i in range(len(schema_encoding)):
                    if schema_encoding[i] == '1':
                        base_record[i+META_COLUMNS] = record[i+META_COLUMNS]

                cols = list()
                for i in range(len(projected_columns_index)):
                    if projected_columns_index[i] == 1:
                        cols.append(base_record[i+META_COLUMNS])
                
                res.append(Record(self.table.key, cols, rid))
                
            return res
            
        except Exception as e:
            return False

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        if len(columns) != self.table.num_columns:
            return False
        
        try:
            if not self.table.update_record(primary_key, columns):
                return False

            return True
            
        except Exception as e:
            print(e)
            return False

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        recs = list()
        sum = 0
        
        if aggregate_column_index >= self.table.num_columns:
            return False
        
        for i in range(start_range, end_range+1):
            try:
                recs.append(self.table.get_record(i))
            except Exception as e:
                continue
            
        if len(recs) == 0:
            return False
        
        for j in recs:
            sum += j[aggregate_column_index]
            
        return sum

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        recs = list()
        sum = 0
        
        if aggregate_column_index >= self.table.num_columns:
            return False
        
        for i in range(start_range, end_range+1):
            try:
                record = self.table.get_record(i)
                
                for j in abs(relative_version):
                    if record[0] is not None:
                        try:
                            record = self.table.get_record(record[0])
                            
                        except Exception as e:
                            continue
                        
                recs.append(record)
                
            except Exception as e:
                continue
            
        if len(recs) == 0:
            return False
        
        for j in recs:
            sum += j[aggregate_column_index]
            
        return sum

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)
        if r is not False and len(r) > 0:
            r = r[0]
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
