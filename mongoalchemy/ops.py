from bson.objectid import ObjectId
from abc import ABCMeta, abstractmethod

class Operation(object):
    __metaclass__ = ABCMeta
    
    execute = abstractmethod(lambda self : None)

    def update_cache(self): pass

    def ensure_indexes(self):
        c = self.collection
        for index in self.type.get_indexes():
            index.ensure(c)


class Save(Operation):
    def __init__(self, session, document, safe):
        self.session = session
        self.data = document.wrap()
        self.type = type(document)
        self.safe = safe
        # Deal with _id
        if '_id' not in self.data:
            self.data['_id'] = ObjectId()
            document.mongo_id = self.data['_id']
    @property
    def collection(self):
        return self.session.db[self.type.get_collection_name()]

    def execute(self):
        self.ensure_indexes()
        self.collection.save(self.data, safe=self.safe)
        
class RemoveObject(Operation):
    def __init__(self, session, obj, safe):
        self.session = session
        self.type = type(obj)
        self.safe = safe
        self.id = None
        if obj.has_id():
            self.id = obj.mongo_id
    @property
    def collection(self):
        return self.session.db[self.type.get_collection_name()]

    def execute(self):
        if self.id is None:
            return
        db = self.session.db
        self.ensure_indexes()

        collection = db[self.type.get_collection_name()]
        collection.remove(self.id, safe=self.safe)

