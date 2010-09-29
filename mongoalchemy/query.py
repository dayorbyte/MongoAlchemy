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

from functools import wraps
from mongoalchemy.fields import BadValueException
from pymongo import ASCENDING, DESCENDING

class BadQueryException(Exception):
    pass

class Query(object):
    def __init__(self, type, db):
        self.db = db
        self.type = type
        self.query = {}
        self.sort = []
        self.__fields = None
    
    def __iter__(self):
        collection = self.db[self.type.get_collection_name()]
        for index in self.type.get_indexes():
            index.ensure(collection)
        
        kwargs = dict()
        if self.__fields:
            kwargs['fields'] = [str(f) for f in self.__fields]
        
        cursor = collection.find(self.query, **kwargs)
        
        if len(self.sort) > 0:
            cursor.sort(self.sort)
        return QueryResult(cursor, self.type, fields=self.__fields)
    
    def filter(self, *query_expressions):
        for qe in query_expressions:
            self.apply(qe)
        return self
    
    def fields(self, *fields):
        if self.__fields == None:
            self.__fields = set()
        for f in fields:
            self.__fields.add(f)
        return self
    
    def apply(self, qe):
        for k, v in qe.obj.iteritems():
            if k not in self.query:
                self.query[k] = v
                continue
            if not isinstance(self.query[k], dict) or not isinstance(v, dict):
                raise BadQueryException('Multiple assignments to a field must all be dicts.')
            self.query[k].update(**v)
    
    def ascending(self, qfield):
        return self.__sort(qfield, ASCENDING)
    
    def descending(self, qfield):
        return self.__sort(qfield, DESCENDING)
    
    def __sort(self, qfield, direction):
        name = str(qfield)
        for n, _ in self.sort:
            if n == name:
                raise BadQueryException('Already sorting by %s' % name)
        self.sort.append((name, direction))
        return self
    
    def not_(self, *query_expressions):
        for qe in query_expressions:
            self.filter(qe.not_())
        return self
    
    def or_(self, first_qe, *qes):
        res = first_qe
        for qe in qes:
            res = (res | qe)
        self.filter(res)
        return self
    
    def in_(self, qfield, *values):
        # TODO: make sure that this field represents a list
        self.filter(QueryExpression({ str(qfield) : { '$in' : values}}))
        return self
    
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
        wrapped = []
        for v in value:
            wrapped.append(qfield.get_type().item_type.wrap(v))
        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_name()] = value
        return self
    
    def atomic_list_op(self, op, qfield, value):
        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_name()] = qfield.get_type().child_type().wrap(value)
        return self
    
    def atomic_op(self, op, qfield, value):
        if op not in self.update_data:
            self.update_data[op] = {}
        self.update_data[op][qfield.get_name()] = qfield.get_type().wrap(value)
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
            raise BadQueryException('%s is not a field in %s' % (name, self.type.class_name()))
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
        fields = self.__type.type.get_fields()
        if name not in fields:
            raise BadQueryException('%s is not a field in %s' % (name, str(self)))
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
    
    def in_(self, *values):
        return QueryExpression({
            str(self) : { '$in' : values }
        })
    
    def __str__(self):
        return self.__absolute_name()
    
    def __eq__(self, value):
        if not self.__type.is_valid_wrap(value):
            raise BadQueryException('Invalid "value" for comparison against %s: %s' % (str(self), value))
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
    
    def __comparator(self, op, value):
        try:
            return QueryExpression({
                self.__absolute_name() : {
                    op : self.__type.wrap(value)
                }
            })
        except BadValueException:
            raise BadQueryException('Invalid "value" for %s comparison against %s: %s' % (self, op, value))

class QueryExpression(object):
    def __init__(self, obj):
        self.obj = obj
    def not_(self):
        return QueryExpression({
                '$not' : self.obj
            })
    
    def __or__(self, expression):
        return self.or_(expression)
    
    def or_(self, expression):
        if '$or' in self.obj:
            self.obj['$or'].append(expression.obj)
            return self
        self.obj = {
            '$or' : [self.obj, expression.obj]
        }
        return self

    
class QueryResult(object):
    def __init__(self, cursor, type, fields=None):
        self.cursor = cursor
        self.type = type
        self.fields = fields
    
    def next(self):
        return self.type.unwrap(self.cursor.next(), fields=self.fields)
    
    def __iter__(self):
        return self


