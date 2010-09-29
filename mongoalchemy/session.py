# The MIT License
# 
# Copyright (c) 2010 Jeffrey Jenkins
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from pymongo.connection import Connection
from mongoalchemy.query import Query

class FailedOperation(Exception):
    def __init__(self, item, exception):
        self.item = item
        self.exception = exception
    def __str__(self):
        return str(self.exception)
    
class Session(object):
    
    def __init__(self, database):
        self.db = database
        self.queue = []
    
    @classmethod
    def connect(self, database, *args, **kwds):
        conn = Connection(*args, **kwds)
        db = conn[database]
        return Session(db)
    
    def end(self):
        self.db.connection.end_request()
    
    def insert(self, item):
        ''' Insert an item into the queue and flushes.  Later this function should be smart and delay 
            insertion until the _id field is actually accessed'''
        self.queue.append(item)
        self.flush()
    
    def execute(self, item):
        self.queue.append(item)
        self.flush()
    
    def query(self, type):
        return Query(type, self.db)
    
    def get_indexes(self, cls):
        return self.db[cls.get_collection_name()].index_information()
    
    def clear(self):
        self.queue = []
    
    def clear_collection(self, *cls):
        for c in cls:
            return self.db[c.get_collection_name()].remove()

    
    def flush(self, safe=True):
        for index, item in enumerate(self.queue):
            item.commit(self.db)
