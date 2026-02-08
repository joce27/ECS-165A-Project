"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        self.indices = [None] *  table.num_columns
        self.indices[table.key] = {}  # Initialize the key column index as a dictionary for O(1) lookups
        

    """
    # returns the location of all records with the given value on column "column"
    """


    def locate(self, column, value):
        if column != self.table.key:
            return []  # Only the key column is indexed for now

            if value in self.indices[column]['index']:
                return self.indices[column]['index'][value]

                return []  # No records found with the given value

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """


    def locate_range(self, begin, end, column):
     if column != self.table.key:
            return []  # Range queries are only supported on the key column for now
            rids = []
            bucket = self.indices[column]['index']
            for key in range(begin, end + 1):
                if key in self.indices[column]['index']:
                    rids.extend(self.indices[column]['index'][key])
            return rids
     
    
        



    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column

    """
    def add_to_index(self, column, value, rid):
        if column == self.table.key:
            # If the column is the key column, we can use the hash index for O(1) lookups
            if value not in self.indices[column]['index']:
                self.indices[column]['index'][value] = []
            self.indices[column]['index'][value].append(rid)



    def remove_from_index(self, column, value, rid):
        if column != self.table.key:
            return  # Only the key column is indexed for now
            # If the column is the key column, we can use the hash index for O(1) lookups
            if value in self.indices[column]:
               del self.indices[column][value]
                  

    def drop_index(self, column_number):
        self.indices[column_number] = None
