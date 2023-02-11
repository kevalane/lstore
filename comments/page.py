
# NOTES FROM SEAN
# pretty important one to implement
# you can play around with a lot of the stuff here
# like max capacity and things like that to see which
# gives best performance
# I think individual columns are stored on individual pages but don't quote me on that
# keep in mind both base pages and tail pages are made up of pages so
# this needs enough abstraction to function for both

# constants to add (prob wanna put this in a config.py file eventually)
#   can put in a config.py file then retrieve them with from config import *
#   also pls consider changing the names of these I used these variable names for my implementation
# MAX_SIZE = X (max number of records in a page)
# REC_SIZE = X (the size of an individual record value in bytes [must be bytes so cause the data is a bytearray])
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    # return true if there is space to store records
    def has_capacity(self):
        # GOOD WAY TO IMPLEMENT
        # if self.num_records >= MAX_SIZE:
        #   return False
        # return True
        pass

    def write(self, value):
        # GOOD WAY TO IMPLEMENT
        # find start and end points of where you want to write
        #   to maximize storage set start point to num_records * REC_SIZE (location of first empty space in the array)
        #   set end point to start + REC_SIZE
        # get the value parameter in byte form
        # data[start:end] = value (byte form)
        self.num_records += 1
        # return something at the end so you can tell the function succeeded
        pass

    # OTHER FUNCTIONS YOU MIGHT WANT TO IMPLEMENT (if you use these, pls change variable names)

    # change the value of a specified record
    def update(self, value, location):
        pass

    # deletes a record from the array (prob set the value to 0 or something)
    def delete(self, location):
        pass

    # read a given record and return it (remember to convert back from bytes if you are returning a value)
    def read(self, location):
        pass
