from pymongo.connection import Connection
from mongomapper.query import Query

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
        conn.end_request()
    
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
    
    def clear(self):
        self.queue = []
    
    def clear_collection(self, cls):
        return self.db[cls.get_collection_name()].remove()

    
    def flush(self, safe=True):
        for index, item in enumerate(self.queue):
            item.commit(self.db)
