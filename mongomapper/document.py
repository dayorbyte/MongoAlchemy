# Copyright (c) 2009, Jeff Jenkins
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * the names of contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY JEFF JENKINS ''AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL JEFF JENKINS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import pymongo

from mongomapper.util import classproperty
from mongomapper.query import Query, QueryFieldSet
from mongomapper.fields import ObjectIdField, Field, ComputedField, BadValueException

class DocumentMeta(type):
    def __new__(meta, classname, bases, class_dict):
        
        new_class = type.__new__(meta, classname, bases, class_dict)
        
        for name, value in class_dict.iteritems():
            if not isinstance(value, Field):
                continue
            value.set_name(name)
            value.set_parent(new_class)
        return new_class

class MissingValueException(Exception):
    pass


class Document(object):
    object_mapping = {}
    
    __metaclass__ = DocumentMeta
    
    _id = ObjectIdField(required=False)
    
    def __init__(self, **kwargs):
        cls = self.__class__
        for name in kwargs:
            if (not hasattr(cls, name) or
                not isinstance(getattr(cls, name), Field)):
                raise Exception('Unknown keyword argument: %s' % name)
            setattr(self, name, kwargs[name])
        
        for name in dir(cls):
            field = getattr(cls, name)
            if isinstance(field, ComputedField):
                setattr(self, name, field.fun(self))
    
    @classproperty
    def f(cls):
        return QueryFieldSet(cls, cls.get_fields())
    
    @classmethod
    def get_fields(cls):
        fields = {}
        for name in dir(cls):
            if name == 'f':
                continue
            field = getattr(cls, name)
            if not isinstance(field, Field):
                continue
            fields[name] = field
        return fields
    
    @classmethod
    def class_name(cls):
        return cls.__name__
    
    @classmethod
    def get_collection_name(cls):
        if not hasattr(cls, '_collection_name'):
            return cls.__name__
        return cls._collection_name
    
    @classmethod
    def get_indexes(cls):
        ret = []
        for name in dir(cls):
            field = getattr(cls, name)
            if isinstance(field, Index):
                ret.append(field)
        return ret
    
    def commit(self, db):
        collection = db[self.get_collection_name()]
        for index in self.get_indexes():
            index.ensure(collection)
        id = collection.save(self.wrap())
        self._id = id
    
    def wrap(self):
        '''Wrap a MongoObject into a format which can be inserted into
            a mongo database'''
        res = {}
        cls = self.__class__
        for name in dir(cls):
            field = getattr(cls, name)
            value = getattr(self, name)
            if isinstance(field, Field):
                if isinstance(value, Field):
                    if field.required:
                        raise MissingValueException(name)
                    continue
                if isinstance(field, ComputedField):
                    value = self
                res[name] = field.wrap(value)
        return res
    
    @classmethod
    def unwrap(cls, obj):
        '''Unwrap an object returned from the mongo database.'''
        
        params = {}
        for k, v in obj.iteritems():
            field = getattr(cls, k)
            if isinstance(field, ComputedField):
                continue
            params[str(k)] = field.unwrap(v)
        
        i = cls(**params)
        return i
    

class DocumentField(Field):
    
    def __init__(self, document_class, **kwargs):
        super(DocumentField, self).__init__(**kwargs)
        self.type = document_class

    def validate_wrap(self, value):
        if not self.is_valid_wrap(value):
            name = self.__class__.__name__
            raise BadValueException('Bad value for field of type "%s(%s)": %s' %
                                    (name, self.type.class_name(), repr(value)))
    def validate_unwrap(self, value):
        if not self.is_valid_unwrap(value):
            name = self.__class__.__name__
            raise BadValueException('Bad value for field of type "%s(%s)": %s' %
                                    (name, self.type.class_name(), repr(value)))

    
    def wrap(self, value):
        self.validate_wrap(value)
        return self.type.wrap(value)
    
    def unwrap(self, value):
        self.validate_unwrap(value)
        return self.type.unwrap(value)
    
    def is_valid_wrap(self, value):
        if not value.__class__ == self.type:
            return False
        # this is super-wasteful
        print 1
        try:
            self.type.wrap(value)
        except:
            return False
        return True
    
    def is_valid_unwrap(self, value):
        # this is super-wasteful
        try:
            self.type.unwrap(value)
        except:
            return False
        return True

class BadIndexException(Exception):
    pass

class Index(object):
    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING
    
    def __init__(self):
        last = None
        self.components = []
        self.__unique = False
        self.__drop_dups = False
    
    def ascending(self, name):
        self.components.append((name, Index.ASCENDING))
        return self

    def descending(self, name):
        self.components.append((name, Index.DESCENDING))
        return self
    
    def unique(self, drop_dups=False):
        self.__unique = True
        self.__drop_dups = drop_dups
        return self
    
    def ensure(self, collection):
        collection.ensure_index(self.components, unique=self.__unique, 
            drop_dups=self.__drop_dups)
        return self
        