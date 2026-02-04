import struct

# each record is a 64-bit integer -> 8 bytes
INT_SIZE = 8 
# maximum number of records per page
MAX_SIZE = 4096 // INT_SIZE

class Page:
    

    def __init__(self):
        self.num_records = 0
        # allocate 4096 bytes of empty space
        self.data = bytearray(4096)
        

    def has_capacity(self):
        return self.num_records < MAX_SIZE
    

    def write(self, value):
        if not self.has_capacity():
            raise RuntimeError("Page is full")
        
        # compute the start index to insert new record at
        start = self.num_records * INT_SIZE
        # insert the integer as 8 bytes with order of bytes being least significant first
        self.data[start:start + INT_SIZE] = value.to_bytes(INT_SIZE, byteorder = 'little')
        
        self.num_records += 1
        
        
    def read(self, value):
        if value < 0 or value >= self.num_records:
            raise RuntimeError("Index out of bounds")
        
        start = value * INT_SIZE
        return int.from_bytes(self.data[start:start + INT_SIZE], byteorder='little')
    

    # might not need for milestone 1
    def update(self, value):
        pass
