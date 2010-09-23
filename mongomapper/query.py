from functools import wraps

class Query(object):
    def __init__(self, type, db):
        self.db = db
        self.type = type
        self.query = {}
    
    def __iter__(self):
        collection = self.db[self.type.get_collection_name()]
        for index in self.type.get_indexes():
            index.ensure(collection)
        cursor = collection.find(self.query)
        return QueryResult(cursor, self.type)
    
    def filter(self, *query_expressions):
        for qe in query_expressions:
            self.apply(qe)
        return self
    
    def apply(self, qe):
        for k, v in qe.obj.iteritems():
            if k not in self.query:
                self.query[k] = v
                continue
            if not isinstance(self.query[k], dict) or not isinstance(v, dict):
                raise Exception('Multiple assignments to a field must all be dicts.')
                self.query[k].update(**v)
    
    def set(self, qfield, value):
        return UpdateExpression(self).set(qfield, value)

class UpdateExpression(object):
    def __init__(self, query):
        self.query = query
        self.update_data = {}
    
    def set(self, qfield, value):
        ''' $set - set a particular value'''
        return self.atomic_op('$set', qfield, value)
    
    def unset(self, qfield, value):
        ''' $unset - delete a particular value (since 1.3.0) 
            TODO: check version is >1.3.0'''
        return self.atomic_op('$set', qfield, value)
        
    def inc(self, qfield, value):
        ''' $inc - increment a particular field by a value '''
        return self.atomic_op('$set', qfield, value)
        
    def append(self, qfield, value):
        ''' $push - append a value to an array'''
        return self.atomic_op('$push', qfield, value)
        
    def extend(self, qfield, *value):
        ''' $pushAll - append several values to an array '''
        return self.atomic_op('$pushAll', qfield, value)
        
    def remove(self, qfield, value):
        ''' $pull - remove a value(s) from an existing array'''
        return self.atomic_op('$pull', qfield, value)
        
    def remove_all(self, qfield, *value):
        ''' $pullAll - remove several value(s) from an existing array'''
        return self.atomic_op('$pullAll', qfield, value)
    
    def add_to_set(self, qfield, *value):
        ''' $pullAll - remove several value(s) from an existing array
            TODO: check version > 1.3.3 '''
        return self.atomic_op('$addToSet', qfield, value)
    
    def pop(self, qfield, value):
        ''' $addToSet - Adds value to the array only if its not in the array already.
            TODO: v1.1 only'''
        return self.atomic_op('$pop', qfield, value)
    
    def atomic_op(self, op, qfield, value):
        if not qfield.type.is_valid(value):
            raise Exception('Invalid "value" for update against %s.%s: %s' % (qfield.type.class_name(), qfield.name, value))
        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.name] = value
        return self
    
    def execute(self):
        assert len(self.update_data) > 0
        collection = self.query.db[self.query.type.get_collection_name()]
        for index in self.query.type.get_indexes():
            index.ensure(collection)
        collection.update(self.query.query, self.update_data)


class QueryFieldSet(object):
    def __init__(self, type, fields, parent=None):
        self.type = type
        self.fields = fields
        self.parent = parent
    def __getattr__(self, name):
        if name not in self.fields:
            raise Exception('%s is not a field in %s' % (name, self.type.class_name()))
        return QueryField(name, self.fields[name], parent=self.parent)

class QueryField(object):
    def __init__(self, name, type, parent=None):
        self.name = name
        self.type = type
        self.parent = parent
    
    @property
    def f(self):
        fields = self.type.type.get_fields()
        return QueryFieldSet(self.type, fields, parent=self)
    
    def absolute_name(self):
        res = []
        current = self
        while current:
            res.append(current.name)
            current = current.parent
        return '.'.join(reversed(res))
    
    def __eq__(self, value):
        if not self.type.is_valid(value):
            raise Exception('Invalid "value" for query against %s.%s: %s' % (self.type.class_name(), name, value))
        return QueryExpression({ self.absolute_name() : value })
    def __lt__(self, value):
        return self.comparator('$lt', value)
    def __le__(self, value):
        return self.comparator('$lte', value)
    def __ne__(self, value):
        return self.comparator('$ne', value)
    def __gt__(self, value):
        return self.comparator('$gt', value)
    def __ge__(self, value):
        return self.comparator('$gte', value)
    def comparator(self, op, value):
        if not self.type.is_valid(value):
            raise Exception('Invalid "value" for query against %s.%s: %s' % (self.type.class_name(), self.absolute_name(), value))
        return QueryExpression({
            self.absolute_name() : {
                op : value
            }
        })

class QueryExpression(object):
    def __init__(self, obj):
        self.obj = obj
    
class QueryResult(object):
    def __init__(self, cursor, type):
        self.cursor = cursor
        self.type = type
    
    def next(self):
        return self.type.unwrap(self.cursor.next())
    
    def __iter__(self):
        return self
    
    
