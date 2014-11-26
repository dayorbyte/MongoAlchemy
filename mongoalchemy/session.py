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
    The primary methods on a session are query, save, and flush.

    The session also responsible for ordering operations and knowing when
    operations need to be flushed, although it does not currently do
    anything intelligent for ordering.
'''

from __future__ import print_function
from mongoalchemy.py3compat import *

import warnings
from uuid import uuid4
import pymongo

if hasattr(pymongo, 'mongo_client'):
    from pymongo.mongo_client import MongoClient
else: # pragma: no cover
    from pymongo.connection import Connection as MongoClient

if hasattr(pymongo, 'mongo_replica_set_client'):
    from pymongo.mongo_replica_set_client import MongoReplicaSetClient

from bson import DBRef, ObjectId
from mongoalchemy.query import Query, QueryResult, RemoveQuery
from mongoalchemy.document import (FieldNotRetrieved, Document,
                                    collection_registry)
from mongoalchemy.query_expression import FreeFormDoc
from mongoalchemy.exceptions import (TransactionException,
                                     BadReferenceException,
                                     SessionCacheException)
from mongoalchemy.ops import *

class Session(object):

    def __init__(self, database, tz_aware=False, timezone=None, safe=False,
                 cache_size=0, auto_ensure=True):
        '''
        Create a session connecting to `database`.

        :param database: the database to connect to.  Should be an instance of \
            :class:`pymongo.database.Database`
        :param safe: Whether the "safe" option should be used on mongo writes, \
            blocking to make sure there are no errors.
        :param auto_ensure: Whether to implicitly call ensure_indexes on all write \
            operations.

        **Fields**:
            * db: the underlying pymongo database object
            * queue: the queue of unflushed database commands (currently useless \
                since there aren't any operations which defer flushing)
            * cache_size: The size of the identity map to keep.  When objects \
                            are pulled from the DB they are checked against this \
                            map and if present, the existing object is used.  \
                            Defaults to 0, use None to only clear at session end.

        '''
        self.db = database
        self.queue = []
        self.safe = safe
        self.timezone = timezone
        self.tz_aware = bool(tz_aware or timezone)
        self.auto_ensure = auto_ensure

        self.cache_size = cache_size
        self.cache = {}
        self.transactions = []
    @property
    def autoflush(self):
        return not self.in_transaction
    @property
    def in_transaction(self):
        return len(self.transactions) > 0

    @classmethod
    def connect(self, database, timezone=None, cache_size=0, auto_ensure=True, replica_set=None, *args, **kwds):
        ''' `connect` is a thin wrapper around __init__ which creates the
            database connection that the session will use.

            :param database: the database name to use.  Should be an instance of \
                    :class:`basestring`
            :param safe: The value for the "safe" parameter of the Session \
                init function
            :param auto_ensure: Whether to implicitly call ensure_indexes on all write \
                operations.
            :param replica_set: The replica-set to use (as a string). If specified, \
                :class:`pymongo.mongo_replica_set_client.MongoReplicaSetClient` is used \
                instead of :class:`pymongo.mongo_client.MongoClient`
            :param args: arguments for :class:`pymongo.mongo_client.MongoClient`
            :param kwds: keyword arguments for :class:`pymongo.mongo_client.MongoClient`
        '''
        safe = kwds.get('safe', False)
        if 'safe' in kwds:
            del kwds['safe']
        if timezone is not None:
            kwds['tz_aware'] = True

        if replica_set is not None:
            if 'MongoReplicaSetClient' in globals():
                conn = MongoReplicaSetClient(*args, replicaSet=replica_set, **kwds)
            else: # pragma: no cover
                conn = MongoClient(*args, replicaSet=replica_set, **kwds)
        else:
            conn = MongoClient(*args, **kwds)

        db = conn[database]
        return Session(db, timezone=timezone, safe=safe, cache_size=cache_size, auto_ensure=auto_ensure)

    def cache_write(self, obj, mongo_id=None):
        if mongo_id is None:
            mongo_id = obj.mongo_id

        if self.cache_size == 0:
            return
        if mongo_id in self.cache:
            return
        if self.cache_size is not None and len(self.cache) >= self.cache_size:
            for key in self.cache:
                break
            del self.cache[key]
        assert isinstance(mongo_id, ObjectId), 'Currently, cached objects must use mongo_id as an ObjectId.  Got: %s' % type(mongo_id)
        self.cache[mongo_id] = obj

    def cache_read(self, id):
        if self.cache_size == 0:
            return
        assert isinstance(id, ObjectId), 'Currently, cached objects must use mongo_id as an ObjectId'
        # if not isinstance(id, ObjectId):
        #     id = ObjectId(id)
        if id in self.cache:
            return self.cache[id]
        return None

    def end(self):
        ''' End the session.  Flush all pending operations and ending the
            *pymongo* request'''
        self.cache = {}
        if self.transactions:
            raise TransactionException('Tried to end session with an open '
                                       'transaction')
        self.db.connection.end_request()

    def insert(self, item, safe=None): # pragma: nocover
        ''' [DEPRECATED] Please use save() instead. This actually calls
            the underlying save function, so the name is confusing.

            Insert an item into the work queue and flushes.'''
        warnings.warn('Insert will be deprecated soon and removed in 1.0. Please use insert',
                      PendingDeprecationWarning)
        self.add(item, safe=safe)

    def save(self, item, safe=None):
        ''' Saves an item into the work queue and flushes.'''
        self.add(item, safe=safe)

    def add(self, item, safe=None):
        ''' Add an item into the queue of things to be inserted.  Does not flush.'''
        item._set_session(self)
        if safe is None:
            safe = self.safe
        self.queue.append(SaveOp(self.transaction_id, self, item, safe))
        # after the save op is recorded, the document has an _id and can be
        # cached
        self.cache_write(item)
        if self.autoflush:
            return self.flush()

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
        if safe is None:
            safe = self.safe
        self.queue.append(UpdateDocumentOp(self.transaction_id, self, item, safe, id_expression=id_expression,
                          upsert=upsert, update_ops=update_ops, **kwargs))
        if self.autoflush:
            return self.flush()

    def query(self, type, exclude_subclasses=False):
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
        return Query(type, self, exclude_subclasses=exclude_subclasses)

    def add_to_session(self, obj):
        obj._set_session(self)

    def execute_query(self, query, session):
        ''' Get the results of ``query``.  This method does flush in a
            transaction, so any objects retrieved which are not in the cache
            which would be updated when the transaction finishes will be
            stale '''
        self.auto_ensure_indexes(query.type)

        kwargs = dict()
        if query._get_fields():
            kwargs['fields'] = query._fields_expression()

        collection = self.db[query.type.get_collection_name()]
        cursor = collection.find(query.query, **kwargs)

        if query._sort:
            cursor.sort(query._sort)
        elif query.type.config_default_sort:
            cursor.sort(query.type.config_default_sort)
        if query.hints:
            cursor.hint(query.hints)
        if query._get_limit() is not None:
            cursor.limit(query._get_limit())
        if query._get_skip() is not None:
            cursor.skip(query._get_skip())
        return QueryResult(session, cursor, query.type, raw_output=query._raw_output, fields=query._get_fields())

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
        if safe is None:
            safe = self.safe
        remove = RemoveDocumentOp(self.transaction_id, self, obj, safe)
        self.queue.append(remove)
        if self.autoflush:
            return self.flush()

    def execute_remove(self, remove):
        ''' Execute a remove expression.  Should generally only be called implicitly.
        '''

        safe = self.safe
        if remove.safe is not None:
            safe = remove.safe

        self.queue.append(RemoveOp(self.transaction_id, self, remove.type, safe, remove))
        if self.autoflush:
            return self.flush()

    def execute_update(self, update, safe=False):
        ''' Execute an update expression.  Should generally only be called implicitly.
        '''

        # safe = self.safe
        # if update.safe is not None:
        #     safe = remove.safe

        assert len(update.update_data) > 0
        self.queue.append(UpdateOp(self.transaction_id, self, update.query.type, safe, update))
        if self.autoflush:
            return self.flush()


    def execute_find_and_modify(self, fm_exp):
        if self.in_transaction:
            raise TransactionException('Cannot find and modify in a transaction.')
        self.flush()
        self.auto_ensure_indexes(fm_exp.query.type)
        # assert len(fm_exp.update_data) > 0
        collection = self.db[fm_exp.query.type.get_collection_name()]
        kwargs = {
            'query' : fm_exp.query.query,
            'update' : fm_exp.update_data,
            'upsert' : fm_exp._get_upsert(),
        }

        if fm_exp.query._get_fields():
            kwargs['fields'] = fm_exp.query._fields_expression()
        if fm_exp.query._sort:
            kwargs['sort'] = fm_exp.query._sort
        if fm_exp._get_new():
            kwargs['new'] = fm_exp._get_new()
        if fm_exp._get_remove():
            kwargs['remove'] = fm_exp._get_remove()

        value = collection.find_and_modify(**kwargs)

        if value is None:
            return None

        # Found this uncommitted.  not sure what it's from? Leaving it
        # until I remember -jeff
        # if kwargs['upsert'] and not kwargs.get('new') and len(value) == 0:
        #     return value

        # No cache in find and modify, right?
        # this is an update operation
        # obj = self.cache_read(value['_id'])
        # if obj is not None:
        #     return obj
        obj = self._unwrap(fm_exp.query.type, value,
                           fields=fm_exp.query._get_fields())
        if not fm_exp.query._get_fields():
            self.cache_write(obj)
        return obj

    def _unwrap(self, type, obj, **kwargs):
        obj = type.transform_incoming(obj, session=self)
        return type.unwrap(obj, session=self, **kwargs)

    @property
    def transaction_id(self):
        if not self.transactions:
            return None
        return self.transactions[-1]

    def get_indexes(self, cls):
        ''' Get the index information for the collection associated with
        `cls`.  Index information is returned in the same format as *pymongo*.
        '''
        return self.db[cls.get_collection_name()].index_information()

    def ensure_indexes(self, cls):
        collection = self.db[cls.get_collection_name()]
        for index in cls.get_indexes():
            index.ensure(collection)

    def auto_ensure_indexes(self, cls):
        if self.auto_ensure:
            self.ensure_indexes(cls)

    def clear_queue(self, trans_id=None):
        ''' Clear the queue of database operations without executing any of
             the pending operations'''
        if not self.queue:
            return
        if trans_id is None:
            self.queue = []
            return

        for index, op in enumerate(self.queue):
            if op.trans_id == trans_id:
                break
        self.queue = self.queue[:index]

    def clear_cache(self):
        self.cache = {}

    def clear_collection(self, *classes):
        ''' Clear all objects from the collections associated with the
            objects in `*cls`. **use with caution!**'''
        for c in classes:
            self.queue.append(ClearCollectionOp(self.transaction_id, self, c))
        if self.autoflush:
            self.flush()

    def flush(self, safe=None):
        ''' Perform all database operations currently in the queue'''
        result = None
        for index, op in enumerate(self.queue):
            try:
                result = op.execute()
            except:
                self.clear_queue()
                self.clear_cache()
                raise
        self.clear_queue()
        return result

    def dereference(self, ref, allow_none=False):
        if isinstance(ref, Document):
            return ref
        if not hasattr(ref, 'type'):
            if ref.collection in collection_registry['global']:
                ref.type = collection_registry['global'][ref.collection]
        assert hasattr(ref, 'type')

        obj = self.cache_read(ref.id)
        if obj is not None:
            return obj
        if ref.database and self.db.name != ref.database:
            db = self.db.connection[ref.database]
        else:
            db = self.db
        value = db.dereference(ref)
        if value is None and allow_none:
            obj = None
            self.cache_write(obj, mongo_id=ref.id)
        elif value is None:
            raise BadReferenceException('Bad reference: %r' % ref)
        else:
            obj = self._unwrap(ref.type, value)
            self.cache_write(obj)
        return obj

    def refresh(self, document):
        """ Load a new copy of a document from the database.  does not
            replace the old one """
        try:
            old_cache_size = self.cache_size
            self.cache_size = 0
            obj = self.query(type(document)).filter_by(mongo_id=document.mongo_id).one()
        finally:
            self.cache_size = old_cache_size
        self.cache_write(obj)
        return obj

    def clone(self, document):
        ''' Serialize a document, remove its _id, and deserialize as a new
            object '''

        wrapped = document.wrap()
        if '_id' in wrapped:
            del wrapped['_id']
        return type(document).unwrap(wrapped, session=self)

    def begin_trans(self):
        self.transactions.append(uuid4())
        return self

    def __enter__(self):
        return self.begin_trans()

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.end_trans(exc_type, exc_val, exc_tb)

    def end_trans(self, exc_type=None, exc_val=None, exc_tb=None):
        # Pop this level of transaction from the stack
        id = self.transactions.pop()

        # If exception, set us as being in an error state
        if exc_type:
            self.clear_queue(trans_id=id)

        # If we aren't at the top level, return
        if self.transactions:
            return False

        if not exc_type:
            self.flush()
            self.end()
        else:
            self.clear_queue()
            self.clear_cache()
        return False

