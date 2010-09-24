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
    
    def sort(self):
        raise NotImplemented
    
    def set(self, qfield, value):
        return UpdateExpression(self).set(qfield, value)
    
    def unset(self, qfield):
        return UpdateExpression(self).unset(qfield)
    
    def inc(self, qfield, value):
        return UpdateExpression(self).inc(qfield, value)
    
    def append(self, qfield, value):
        return UpdateExpression(self).append(qfield, value)
    
    def extend(self, qfield, *value):
        return UpdateExpression(self).extend(qfield, *value)
    
    def remove(self, qfield, value):
        return UpdateExpression(self).remove(qfield, value)
    
    def remove_all(self, qfield, *value):
        return UpdateExpression(self).remove_all(qfield, *value)
        
    def add_to_set(self, qfield, value):
        return UpdateExpression(self).add_to_set(qfield, value)
        
    def pop(self, qfield, value):
        return UpdateExpression(self).pop(qfield, value)

class UpdateExpression(object):
    def __init__(self, query):
        self.query = query
        self.update_data = {}
    
    def set(self, qfield, value):
        ''' $set - set a particular value'''
        return self.atomic_op('$set', qfield, value)
    
    def unset(self, qfield):
        ''' $unset - delete a particular value (since 1.3.0) 
            TODO: check version is >1.3.0'''
        return self.atomic_op('$unset', qfield, True)
        
    def inc(self, qfield, value):
        ''' $inc - increment a particular field by a value '''
        return self.atomic_op('$inc', qfield, value)
        
    def append(self, qfield, value):
        ''' $push - append a value to an array'''
        return self.atomic_list_op('$push', qfield, value)
        
    def extend(self, qfield, *value):
        ''' $pushAll - append several values to an array '''
        return self.atomic_list_op_multivalue('$pushAll', qfield, *value)
        
    def remove(self, qfield, value):
        ''' $pull - remove a value(s) from an existing array'''
        return self.atomic_list_op('$pull', qfield, value)
        
    def remove_all(self, qfield, *value):
        ''' $pullAll - remove several value(s) from an existing array'''
        return self.atomic_list_op_multivalue('$pullAll', qfield, *value)
    
    def add_to_set(self, qfield, value):
        ''' $pullAll - remove several value(s) from an existing array
            TODO: check version > 1.3.3 '''
        return self.atomic_list_op('$addToSet', qfield, value)
    
    def pop(self, qfield, value):
        ''' $addToSet - Adds value to the array only if its not in the array already.
            TODO: v1.1 only'''
        return self.atomic_list_op('$pop', qfield, value)
    
    def atomic_list_op_multivalue(self, op, qfield, *value):
        for v in value:
            if not qfield.get_type().is_valid_child(v):
                raise Exception('Invalid "value" for update against %s.%s: %s' % (qfield.get_type().parent().class_name(), qfield.get_name(), value))
        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_name()] = value
        return self
    
    def atomic_list_op(self, op, qfield, value):
        if not qfield.get_type().is_valid_child(value):
            raise Exception('Invalid "value" for update against %s.%s: %s' % (qfield.get_type().parent().class_name(), qfield.get_name(), value))
        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_name()] = value
        return self
    
    def atomic_op(self, op, qfield, value):
        if not qfield.get_type().is_valid(value):
            raise Exception('Invalid "value" for update against %s.%s: %s' % (qfield.get_type().parent().class_name(), qfield.get_name(), value))
        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_name()] = value
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
        self.__name = name
        self.__type = type
        self.__parent = parent
    
    def get_parent(self):
        return self.__parent
    
    def get_name(self):
        return self.__name
    
    def get_type(self):
        return self.__type
    
    def __getattr__(self, name):
        # if name.startswith('__'):
        #     return object.__getattribute__(self, name)
        fields = self.__type.type.get_fields()
        if name not in fields:
            raise Exception('%s is not a field in %s' % (name, self.__type.class_name()))
        return QueryField(name, fields[name], parent=self)
    
    @property
    def f(self):
        fields = self.__type.type.get_fields()
        return QueryFieldSet(self.__type, fields, parent=self)
    
    def __absolute_name(self):
        res = []
        current = self
        while current:
            res.append(current.__name)
            current = current.__parent
        return '.'.join(reversed(res))
    
    def __str__(self):
        return self.__absolute_name()
    
    def __eq__(self, value):
        if not self.__type.is_valid(value):
            raise Exception('Invalid "value" for query against %s.%s: %s' % (self.type.class_name(), name, value))
        return QueryExpression({ self.__absolute_name() : value })
    def __lt__(self, value):
        return self.__comparator('$lt', value)
    def __le__(self, value):
        return self.__comparator('$lte', value)
    def __ne__(self, value):
        return self.__comparator('$ne', value)
    def __gt__(self, value):
        return self.__comparator('$gt', value)
    def __ge__(self, value):
        return self.__comparator('$gte', value)
    
    def in_(self, value):
        # TODO: make sure that this field represents a list
        return self.__comparator('$in', value)
    
    def not_(self, expression):
        raise NotImplemented
    
    def or_(self, expression):
        raise NotImplemented
    
    def regex(self, value):
        raise NotImplemented
    
    def __comparator(self, op, value):
        if not self.__type.is_valid(value):
            raise Exception('Invalid "value" for query against %s.%s: %s' % (self.__type.class_name(), self.__absolute_name(), value))
        return QueryExpression({
            self.__absolute_name() : {
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
    
    
