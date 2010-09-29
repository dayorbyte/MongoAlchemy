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

import pymongo

from mongoalchemy.util import classproperty
from mongoalchemy.query import QueryFieldSet
from mongoalchemy.fields import ObjectIdField, Field, BadValueException

class DocumentMeta(type):
    def __new__(mcs, classname, bases, class_dict):
        new_class = type.__new__(mcs, classname, bases, class_dict)
        
        for name, value in class_dict.iteritems():
            if not isinstance(value, Field):
                continue
            value.set_name(name)
            value.set_parent(new_class)

        return new_class

class DocumentException(Exception):
    ''' Base for all document-related exceptions'''
    pass

class MissingValueException(DocumentException):
    ''' Raised when a required field isn't set '''
    pass

class ExtraValueException(DocumentException):
    ''' Raised when a value is passed in with no corresponding field '''
    pass

class FieldNotRetrieved(DocumentException):
    '''If a partial document is loaded from the database and a field which 
        wasn't retrieved is accessed this exception is raised'''
    pass

class Document(object):
    object_mapping = {}
    
    __metaclass__ = DocumentMeta
    
    _id = ObjectIdField(required=False)
    
    def __init__(self, retrieved_fields=None, **kwargs):
        self.partial = retrieved_fields != None
        self.retrieved_fields = self.normalize(retrieved_fields)
        
        cls = self.__class__
                
        fields = self.get_fields()
        for name, field in fields.iteritems():
            
            if self.partial and name not in self.retrieved_fields:
                continue
            
            if name in kwargs:
                setattr(self, name, kwargs[name])
                continue
            
            if field.auto:
                continue
            
            if field.required:
                raise MissingValueException(name)
            
            if hasattr(field, 'default'):
                setattr(self, name, field.default)
        
        for k in kwargs:
            if k not in fields:
                raise ExtraValueException(k)
    
    def __setattr__(self, name, value):
        cls = self.__class__
        try:
            cls_value = getattr(cls, name)
            if isinstance(cls_value, Field):
                cls_value.validate_wrap(value)
        except AttributeError:
            pass
        object.__setattr__(self, name, value)
    
    def __getattribute__(self, name):
        if name[:2] == '__':
            return object.__getattribute__(self, name)
        value = object.__getattribute__(self, name)
        cls_value = object.__getattribute__(self, name)
        
        if isinstance(cls_value, Field):
            partial = object.__getattribute__(self, 'partial')
            retrieved = object.__getattribute__(self, 'retrieved_fields')
            if partial and name not in retrieved:
                raise FieldNotRetrieved(name)
        
        if isinstance(value, Field):
            raise AttributeError(name)
        return value
    
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
    
    @classmethod
    def normalize(cls, fields):
        if not fields:
            return fields
        ret = {}
        for f in fields:
            strf = str(f)
            if '.' in strf:
                first, _, second = strf.partition('.')
                ret.setdefault(first, []).append(second)
            else:
                ret[strf] = None
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
            try:
                value = getattr(self, name)
            except AttributeError:
                continue
            if isinstance(field, Field):
                res[name] = field.wrap(value)
        return res
    
    @classmethod
    def unwrap(cls, obj, fields=None):
        '''Unwrap an object returned from the mongo database.'''
        params = {}        
        for k, v in obj.iteritems():
            field = getattr(cls, k)
            if fields != None and isinstance(field, DocumentField):
                normalized_fields = cls.normalize(fields)
                unwrapped = field.unwrap(v, fields=normalized_fields.get(k))
            else:
                unwrapped = field.unwrap(v)
            params[str(k)] = unwrapped
        
        if fields != None:
            params['retrieved_fields'] = fields
        return cls(**params)

class DocumentField(Field):
    
    def __init__(self, document_class, **kwargs):
        super(DocumentField, self).__init__(**kwargs)
        self.type = document_class

    def validate_wrap(self, value):
        if not self.is_valid_wrap(value):
            name = self.__class__.__name__
            raise BadValueException('Bad value for field of type "%s(%s)": %s' %
                                    (name, self.type.class_name(), repr(value)))
    def validate_unwrap(self, value, fields=None):
        if not self.is_valid_unwrap(value, fields=fields):
            name = self.__class__.__name__
            raise BadValueException('Bad value for field of type "%s(%s)": %s' %
                                    (name, self.type.class_name(), repr(value)))

    
    def wrap(self, value):
        self.validate_wrap(value)
        return self.type.wrap(value)
    
    def unwrap(self, value, fields=None):
        self.validate_unwrap(value, fields=fields)
        return self.type.unwrap(value, fields=fields)
    
    def is_valid_wrap(self, value):
        # we've validated everything we set on the object, so this should 
        # always return True if it's the right kind of object
        return value.__class__ == self.type
    
    def is_valid_unwrap(self, value, fields=None):
        # this is super-wasteful
        try:
            self.type.unwrap(value, fields=fields)
        except:
            return False
        return True

class BadIndexException(Exception):
    pass

class Index(object):
    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING
    
    def __init__(self):
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
        