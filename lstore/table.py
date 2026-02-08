from lstore.index import Index
from time import time
from lstore.page import Page

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

MAX_BASE_PAGES = 16

class PageRange:
    
    
    """
    :param max_base_pages: int     #maximum number of base pages allowed
    :param num_col: int            #number of columns in table
    """
    def __init__(self, num_col, max_base_pages):
        # add 4 to account for metadata columns
        self.num_col = 4 + num_col
        self.max_base_pages = max_base_pages
        
        # each column gets a list of pages
        self.base_pages = [[] for _ in range(num_col)]
        self.tail_pages = [[] for _ in range(num_col)]
        
        # add first tail page for each column
        for col in range(num_col):
            self.tail_pages[col].append(Page())
        
        
    # check if base page range has capacity
    def base_has_capacity(self):
        # if any column is full, then the page range is full
        return len(self.base_pages[0]) < self.max_base_pages
    
    def add_base_page(self):
        if not self.base_has_capacity():
            raise RuntimeError("Base page range is full")
        
        # add base pages for each column
        for col in range(self.num_col):
            self.base_pages[col].append(Page())
        
    def add_tail_page(self, col):
         # add tail page for a column if last tail page is full
        last_tail_page = self.tail_pages[col][-1]
        if not last_tail_page.has_capacity():
            self.tail_pages[col].append(Page())
            
        
class Record:
    
    
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        

class Table:


    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = 4 + num_columns
        self.page_directory = {}
        self.tail_page_directory = {}
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.page_ranges = []
        self.rid_counter = 0
        
        
    """
    :param record: list[int]     #list of column values to be inserted
    """     
    def insert(self, record):
        rid = self.rid_counter
        # increment rid_counter to ensure RID uniqueness for every insert
        self.rid_counter += 1
        
        indirection = 0
        timestamp = int(time())
        schema_encoding = 0
        
        record = [rid, indirection, schema_encoding, timestamp] + list(record)
        
        # create page range if there isn't one or if last page range is full
        if not self.page_ranges or not self.page_ranges[-1].base_has_capacity():
            self.page_ranges.append(PageRange(self.num_columns, MAX_BASE_PAGES))
        
        last_page_range = self.page_ranges[-1]
        # if no page or page capacity, create new base page
        if not last_page_range.base_pages[0] or not last_page_range.base_pages[0][-1].has_capacity():
            last_page_range.add_base_page()
        
        # write each value into its column's last base page
        for col, value in enumerate(record):
            last_page = last_page_range.base_pages[col][-1]
            last_page.write(value)
            
        # update page directory
        page_range_ind = len(self.page_ranges) - 1
        page_ind = len(last_page_range.base_pages[0]) - 1
        offset = last_page_range.base_pages[0][-1].num_records - 1
        self.page_directory[rid] = (page_range_ind, page_ind, offset)
    
    
    """
    :param rid: int
    :param col: int     #column number in table
    """ 
    def read(self, rid, col):
        # get record locaiton
        page_range_ind, page_ind, offset = self.page_directory[rid]
        page_range = self.page_ranges[page_range_ind]
        base_page = page_range.base_pages[col][page_ind]
        
        # check indirection column
        indirection_page = page_range.base_pages[INDIRECTION_COLUMN][page_ind]
        tail_rid = indirection_page.read(offset)
        # no tail record, read from base page
        if tail_rid in [0, None]:
            return base_page.read(offset)
    
        # tail record exists, read from tail page
        tail_page_range_ind, tail_page_ind, tail_offset = self.tail_page_directory[tail_rid]
        tail_page_range = self.page_ranges[tail_page_range_ind]
        tail_page = tail_page_range.tail_pages[col][tail_page_ind]
        
        # check schema encoding for udated columns
        schema_page = tail_page_range.base_pages[SCHEMA_ENCODING_COLUMN][tail_page_ind]
        schema_bitmap = schema_page.read(tail_offset)
        bit_ind = col - 4
        if (schema_bitmap >> bit_ind) & 1:
            return tail_page.read(tail_offset)
        else:
            return base_page.read(offset)
        
        
    """
    :param rid: int
    :param *cols: tuple     #updated column values
    """  
    def update(self, rid, *cols):
        # get record location
        page_range_ind, page_ind, offset = self.page_directory[rid]
        page_range = self.page_ranges[page_range_ind]
        
        base_indirection_page = page_range.base_pages[INDIRECTION_COLUMN][page_ind]
        # 0 indicates no tail record
        tail_rid = base_indirection_page.read(offset)
        
        new_tail_rid = self.rid_counter
        self.rid_counter += 1
        
        timestamp = int(time())
        indirection = tail_rid
        schema_bitmap = 0
        tail_record = [new_tail_rid, indirection, schema_bitmap, timestamp]
        
        # update columns in tail record and update schema encoding bitmap
        for i , val in enumerate(cols):
            if val is not None:
                schema_bitmap |= 1 << i
                tail_record.append(val)
            else:
                tail_record.append(0)
        
        tail_record[SCHEMA_ENCODING_COLUMN] = schema_bitmap
        
        # write tail record to tail page (create new tail page if latest one has no capacity)
        for col, value in enumerate(tail_record):
            last_tail_page = page_range.tail_pages[col][-1]
            if not last_tail_page.has_capacity():
                page_range.add_tail_page(col)
                last_tail_page = page_range.tail_pages[col][-1]
            last_tail_page.write(value)
            
        # update tail page directory
        page_range_ind = len(self.page_ranges) - 1
        page_ind = len(page_range.tail_pages[0]) - 1
        offset = page_range.tail_pages[0][-1].num_records - 1
        self.page_directory[tail_rid] = (page_range_ind, page_ind, offset)
        
    
    # Leave for milestone 2
    def __merge(self):
        print("merge is happening")
        pass
 
