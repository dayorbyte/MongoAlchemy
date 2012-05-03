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
from mongoalchemy.query import Query, QueryResult, RemoveQuery
from mongoalchemy.document import FieldNotRetrieved
from mongoalchemy.query_expression import FreeFormDoc
from itertools import chain

class Session(object):

    def __init__(self, database, safe=False):
        '''
        Create a session connecting to `database`.  
        
        :param database: the database to connect to.  Should be an instance of \
            :class:`pymongo.database.Database`
        :param safe: Whether the "safe" option should be used on mongo writes, \
            blocking to make sure there are no errors.
        
        **Fields**:
            * db: the underlying pymongo database object
            * queue: the queue of unflushed database commands (currently useless \
                since there aren't any operations which defer flushing)
        '''
        self.db = database
        self.queue = []
        self.safe = safe
    
    @classmethod
    def connect(self, database, *args, **kwds):
        ''' `connect` is a thin wrapper around __init__ which creates the 
            database connection that the session will use.
            
            :param database: the database name to use.  Should be an instance of \
                    :class:`basestring`
            :param safe: The value for the "safe" parameter of the Session \ 
                init function
            :param args: arguments for :class:`pymongo.connection.Connection`
            :param kwds: keyword arguments for :class:`pymongo.connection.Connection`
        '''
        safe = kwds.get('safe', False)
        if 'safe' in kwds:
            del kwds['safe']
        conn = Connection(*args, **kwds)
        db = conn[database]
        return Session(db, safe=safe)
    
    def end(self):
        ''' End the session.  Flush all pending operations and ending the 
            *pymongo* request'''
        self.flush()
        self.db.connection.end_request()
    
    def insert(self, item, safe=None):
        ''' Insert an item into the queue and flushes.  Later this function should be smart and delay 
            insertion until the _id field is actually accessed'''
        if safe is None:
            safe = self.safe
        self.queue.append(item)
        self.flush(safe=safe)
    
    def update(self, item, id_expression=None, upsert=False, update_ops={}, safe=None, **kwargs):
        ''' Update an item in the database.  Uses the on_update keyword to each
            field to decide which operations to do, or.  
            
            :param item: An instance of a :class:`~mongoalchemy.document.Document` \
                subclass
            :param id_expression: A query expression that uniquely picks out \
                the item which should be updated.  If id_expression is not \
                passed, update uses item.mongo_id.
            :param upsert: Whether the update operation should be an upsert. \
                If the item may not be in the database yet this should be True
            :param update_ops: By default the operation used to update a field \
                is specified with the on_update argument to its constructor. \
                To override that value, use this dictionary, with  \
                :class:`~mongoalchemy.document.QueryField` objects as the keys \
                and the mongo operation to use as the values.
            :param kwargs: The kwargs are merged into update_ops dict to \
                decide which fields to update the operation for.  These can \
                only be for the top-level document since the keys \
                are just strings.
            
            .. warning::
                
                This operation is **experimental** and **not fully tested**,
                although it does have code coverage.  
            '''
        if id_expression:
            db_key = Query(type(item), self).filter(id_expression).query
        else:
            db_key = {'_id' : item.mongo_id}

        dirty_ops = item.get_dirty_ops(with_required=upsert)
        for key, op in chain(update_ops.items(), kwargs.items()):
            key = str(key)
            for current_op, keys in dirty_ops.items():
                if key not in keys:
                    continue
                dirty_ops.setdefault(op,{})[key] = keys[key]
                del dirty_ops[current_op][key]
                if len(dirty_ops[current_op]) == 0:
                    del dirty_ops[current_op]
        if safe is None:
            safe = self.safe
        self.flush(safe=safe)
        self.db[item.get_collection_name()].update(db_key, dirty_ops, upsert=upsert, safe=safe)
        
    
    def query(self, type):
        ''' Begin a query on the database's collection for `type`.  If `type`
            is an instance of basesting, the query will be in raw query mode
            which will not check field values or transform returned results
            into python objects.
        
         .. seealso:: :class:`~mongoalchemy.query.Query` class'''
        # This really should be adding a query operation to the 
        # queue which is then forced to execute when the results are being
        # read
        if isinstance(type, basestring):
            type = FreeFormDoc(type)
        return Query(type, self)
    
    def execute_query(self, query):
        ''' Get the results of ``query``.  This method will flush the queue '''
        collection = self.db[query.type.get_collection_name()]
        for index in query.type.get_indexes():
            index.ensure(collection)
        
        kwargs = dict()
        if query.get_fields():
            kwargs['fields'] = [str(f) for f in query.get_fields()]
        
        cursor = collection.find(query.query, **kwargs)
        
        if query.sort:
            cursor.sort(query.sort)
        if query.hints:
            cursor.hint(query.hints)
        if query.get_limit() != None:
            cursor.limit(query.get_limit())
        if query.get_skip() != None:
            cursor.skip(query.get_skip())
        return QueryResult(cursor, query.type, raw_output=query._raw_output, fields=query.get_fields())
    
    def remove_query(self, type):
        ''' Begin a remove query on the database's collection for `type`.
  
           .. seealso:: :class:`~mongoalchemy.update_expression.RemoveQuery` class'''
        return RemoveQuery(type, self)
    
    def remove(self, obj, safe=None):
        '''
            Remove a particular object from the database.  If the object has 
            no mongo ID set, the method just returns.  If this is a partial 
            document without the mongo ID field retrieved a ``FieldNotRetrieved``
            will be raised
            
            :param obj: the object to save
            :param safe: whether to wait for the operation to complete.  Defaults \
                to the session's ``safe`` value.
        '''
        self.flush()
        if safe is None:
            safe = self.safe
        if not obj.has_id():
            return None
        collection = self.db[obj.get_collection_name()]
        for index in obj.get_indexes():
            index.ensure(collection)
        return self.db[obj.get_collection_name()].remove(obj.mongo_id, safe=safe)
    
    def execute_remove(self, remove):
        ''' Execute a remove expression.  Should generally only be called implicitly.
        '''
        self.flush()
        safe = self.safe
        if remove.safe != None:
            safe = remove.safe
        
        collection = self.db[remove.type.get_collection_name()]
        for index in remove.type.get_indexes():
            index.ensure(collection)

        return self.db[remove.type.get_collection_name()].remove(remove.query, safe=safe)
    
    def execute_update(self, update, safe=False):
        ''' Execute an update expression.  Should generally only be called implicitly.
        '''
        
        self.flush()
        assert len(update.update_data) > 0
        collection = self.db[update.query.type.get_collection_name()]
        for index in update.query.type.get_indexes():
            index.ensure(collection)
        kwargs = dict(
            upsert=update.get_upsert(), 
            multi=update.get_multi(),
            safe=safe,
        )
        print kwargs
        collection.update(update.query.query, update.update_data, **kwargs)
    
    def execute_find_and_modify(self, fm_exp):
        self.flush()
        # assert len(fm_exp.update_data) > 0
        collection = self.db[fm_exp.query.type.get_collection_name()]
        for index in fm_exp.query.type.get_indexes():
            index.ensure(collection)
        kwargs = {
            'query' : fm_exp.query.query, 
            'update' : fm_exp.update_data, 
            'upsert' : fm_exp.get_upsert(), 
        }
        
        if fm_exp.query.get_fields():
            kwargs['fields'] = {}
            for f in fm_exp.query.get_fields():
                kwargs['fields'][str(f)] = True
        if fm_exp.query.sort:
            kwargs['sort'] = fm_exp.query.sort
        if fm_exp.get_new():
            kwargs['new'] = fm_exp.get_new()
        if fm_exp.get_remove():
            kwargs['remove'] = fm_exp.get_remove()
        
        value = collection.find_and_modify(**kwargs)        
        
        if value is None:
            return None
        
        if kwargs['upsert'] and not kwargs.get('new') and len(value) == 0:
            return value
        
        return fm_exp.query.type.unwrap(value, fields=fm_exp.query.get_fields())
    
    def get_indexes(self, cls):
        ''' Get the index information for the collection associated with 
        `cls`.  Index information is returned in the same format as *pymongo*.
        '''
        return self.db[cls.get_collection_name()].index_information()
    
    def clear(self):
        ''' Clear the queue of database operations without executing any of 
             the pending operations'''
        self.queue = []
    
    def clear_collection(self, *classes):
        ''' Clear all objects from the collections associated with the 
            objects in `*cls`. **use with caution!**'''
        for c in classes:
            self.db[c.get_collection_name()].remove()
    
    def flush(self, safe=None):
        ''' Perform all database operations currently in the queue'''
        if safe is None:
            safe = self.safe
        for index, item in enumerate(self.queue):
            try:
                item.commit(self.db, safe=safe)
            except:
                self.clear()
                raise
        self.clear()
            
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end()
        return False
