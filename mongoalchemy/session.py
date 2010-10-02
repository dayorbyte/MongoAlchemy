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

''' Session objects handles the actual queueing of database operations.
    The primary methods on a session are query, insert, and flush.
    
    The preferred way to use a session is the `with statement`::
        
        s = Session(some_db)
        with s:
            s.insert(some_obj)
            obj = s.query(SomeClass).one()
            ...
    
    The with statement ensures that end_request is called on the 
    connection which can have a significant performance impact in some 
    situations because it will allow other threads to make use of the 
    `socket` connecting to the database.
    
    The session also responsible for ordering operations and knowing when
    operations need to be flushed, although it does not currently do
    anything intelligent for ordering.
'''


from pymongo.connection import Connection
from mongoalchemy.query import Query

class Session(object):

    def __init__(self, database):
        '''
        Create a session connecting to `database`
        
        **Parameters**:
            * `database`: the database to connect to.  Should be an instance of \
                :class:`pymongo.collection.Collection`
        
        '''
        self.db = database
        self.queue = []
    
    @classmethod
    def connect(self, database, *args, **kwds):
        ''' `connect` is a thin wrapper around __init__ which creates the 
            database connection that the session will use.
            
            **Parameters**:
            * `database`: the database name to use.  Should be an instance of \
                :class:`basestring`
            * `*args`: arguments for :class:`pymongo.connection.Connection`
            * `*kwds`: keyword arguments for :class:`pymongo.connection.Connection`

        '''
        conn = Connection(*args, **kwds)
        db = conn[database]
        return Session(db)
    
    def end(self):
        ''' End the session.  Flush all pending operations and ending the 
        *pymongo* request'''
        self.flush()
        self.db.connection.end_request()
    
    def insert(self, item):
        ''' Insert an item into the queue and flushes.  Later this function should be smart and delay 
            insertion until the _id field is actually accessed'''
        self.queue.append(item)
        self.flush()
    
    def query(self, type):
        ''' Begin a query on the database's collection for `type`.
        
         .. seealso:: :class:`~mongoalchemy.query.Query` class'''
        # This really should be adding a query operation to the 
        # queue which is then forced to execute when the results are being
        # read
        return Query(type, self.db)
    
    def get_indexes(self, cls):
        ''' Get the index information for the collection associated with 
        `cls`.  Index information is returned in the same format as *pymongo*.
        '''
        return self.db[cls.get_collection_name()].index_information()
    
    def clear(self):
        ''' Clear the queue of database operations without executing any of 
             the pending operations'''
        self.queue = []
    
    def clear_collection(self, *cls):
        ''' Clear all objects from the collections associated with the 
            objects in `*cls`. **use with caution!**'''
        
        for c in cls:
            return self.db[c.get_collection_name()].remove()
    
    def flush(self, safe=True):
        ''' Perform all database operations currently in the queue'''
        for index, item in enumerate(self.queue):
            item.commit(self.db)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end()
        return False
