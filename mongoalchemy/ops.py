from __future__ import print_function
from mongoalchemy.py3compat import *

from itertools import chain
from bson.objectid import ObjectId
from abc import ABCMeta, abstractmethod
from mongoalchemy.exceptions import InvalidUpdateException

@add_metaclass(ABCMeta)
class Operation(object):
    __metaclass__ = ABCMeta

    execute = abstractmethod(lambda self : None)

    def update_cache(self): pass

    @property
    def collection(self):
        return self.session.db[self.type.get_collection_name()]

    def ensure_indexes(self):
        self.session.auto_ensure_indexes(self.type)

class ClearCollectionOp(Operation):
    def __init__(self, trans_id, session, kind):
        self.trans_id = trans_id
        self.session = session
        self.type = kind
    def execute(self):
        self.collection.remove()

class UpdateDocumentOp(Operation):
    def __init__(self, trans_id, session, document, safe, id_expression=None, upsert=False, update_ops={}, **kwargs):
        from mongoalchemy.query import Query
        self.session = session
        self.trans_id = trans_id
        self.type = type(document)
        self.safe = safe
        self.upsert = upsert

        if id_expression:
            self.db_key = Query(self.type, session).filter(id_expression).query
        elif document.has_id():
            self.db_key = {'_id' : document.mongo_id}
        else:
            raise InvalidUpdateException('To upsert the document must have a '
                ' mongo_id OR id_expression must be specified')


        self.dirty_ops = document.get_dirty_ops(with_required=upsert)
        for key, op in chain(update_ops.items(), kwargs.items()):
            key = str(key)
            for current_op, keys in list(self.dirty_ops.items()):
                if key not in keys:
                    continue
                self.dirty_ops.setdefault(op,{})[key] = keys[key]
                del self.dirty_ops[current_op][key]
                if len(self.dirty_ops[current_op]) == 0:
                    del self.dirty_ops[current_op]
        document._mark_clean()

    def execute(self):
        self.ensure_indexes()
        return self.collection.update(self.db_key, self.dirty_ops, upsert=self.upsert, safe=self.safe)

class UpdateOp(Operation):
    def __init__(self, trans_id, session, kind, safe, update_obj):
        self.session = session
        self.trans_id = trans_id
        self.type = kind
        self.safe = safe
        self.query = update_obj.query.query
        self.update_data = update_obj.update_data
        self.upsert = update_obj._get_upsert()
        self.multi = update_obj._get_multi()

    def execute(self):
        return self.collection.update(self.query, self.update_data, multi=self.multi,
                               upsert=self.upsert, safe=self.safe)


class SaveOp(Operation):
    def __init__(self, trans_id, session, document, safe):
        self.session = session
        self.trans_id = trans_id
        self.data = document.wrap()
        self.type = type(document)
        self.safe = safe
        # Deal with _id
        if '_id' not in self.data:
            self.data['_id'] = ObjectId()
            document.mongo_id = self.data['_id']
        document._mark_clean()

    def execute(self):
        self.ensure_indexes()
        return self.collection.save(self.data, safe=self.safe)

class RemoveOp(Operation):
    def __init__(self, trans_id, session, kind, safe, query):
        self.session = session
        self.trans_id = trans_id
        self.query = query.query
        self.safe = safe
        self.type = kind

    def execute(self):
        self.ensure_indexes()
        return self.collection.remove(self.query, safe=self.safe)


class RemoveDocumentOp(Operation):
    def __init__(self, trans_id, session, obj, safe):
        self.trans_id = trans_id
        self.session = session
        self.type = type(obj)
        self.safe = safe
        self.id = None
        if obj.has_id():
            self.id = obj.mongo_id

    def execute(self):
        if self.id is None:
            return
        db = self.session.db
        self.ensure_indexes()

        collection = db[self.type.get_collection_name()]
        return collection.remove(self.id, safe=self.safe)
