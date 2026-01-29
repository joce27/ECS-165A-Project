import struct

int_size = 8
max_size = 4096 // int_size

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < max_size

    def write(self, value):
        if not self.has_capacity()
            raise RuntimeError("Page is full")
        
        self.num_records += 1
        pass

