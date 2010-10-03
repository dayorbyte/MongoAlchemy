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
'''

A `mongoalchemy` document is used to define a mapping between a python object 
and a document in a Mongo Database.  Mappings are defined by creating a 
subclass of :class:`Document` with attributes to define 
what maps to what.  The two main types of attributes are :class:`~mongoalchemy.fields.Field` 
and :class:`Index`

A :class:`~mongoalchemy.fields.Field` is used to define the type of a field in
mongo document, any constraints on the values, and to provide methods for 
transforming a value from a python object into something Mongo understands and 
vice-versa.

A :class:`~Index` is used to define an index on the underlying collection 
programmatically.  A document can have multiple indexes by adding extra 
:class:`~Index` attributes


'''
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
            value._set_name(name)
            value._set_parent(new_class)
        new_class.f = QueryFieldSet(new_class, new_class.get_fields())
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
    __metaclass__ = DocumentMeta
    
    _id = ObjectIdField(required=False)
    
    def __init__(self, retrieved_fields=None, **kwargs):
        '''
        **Parameters**:
            * retrieved_fields: The names of the fields returned when loading \
                a partial object.  This argument should not be explicitly set \
                by subclasses
            * \*\*kwargs:  The values for all of the fields in the document. \
                Any additional fields will raise a :class:`~mongoalchemy.document.ExtraValueException` and \ 
                any missing (but required) fields will raise a :class:`~mongoalchemy.document.MissingValueException`. \
                Both types of exceptions are subclasses of :class:`~mongoalchemy.document.DocumentException`.
        '''
        self.partial = retrieved_fields != None
        self.retrieved_fields = self.__normalize(retrieved_fields)
        
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
    
    f = None
    '''The ``f`` attribute of a document class allows the creation of
        :class:`~mongoalchemy.query.QueryField` objects to be used in query
        expressions, updates, loading partial documents, and a number of other 
        places.
        
        .. seealso:: :class:`~mongoalchemy.query.QueryExpression`, :class:`~mongoalchemy.query.Query`
    
    '''
    
    @classmethod
    def get_fields(cls):
        '''Returns a dict mapping the names of the fields in a document 
            or subclass to the associated :class:`~mongoalchemy.fields.Field`
        '''
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
        '''Returns the name of the class. The name of the class is also the 
            default collection name.  
            
            .. seealso:: :func:`~Document.get_collection_name`
        '''
        return cls.__name__
    
    @classmethod
    def get_collection_name(cls):
        ''' Returns the collection name used by the class.  If the ``_collection_name``
            attribute is set it is used, otherwise the name of the class is used.'''
        if not hasattr(cls, '_collection_name'):
            return cls.__name__
        return cls._collection_name
    
    @classmethod
    def get_indexes(cls):
        ''' Returns all of the :class:`~mongoalchemy.document.Index` instances
            for the current class.'''
        ret = []
        for name in dir(cls):
            field = getattr(cls, name)
            if isinstance(field, Index):
                ret.append(field)
        return ret
    
    @classmethod
    def __normalize(cls, fields):
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
        ''' Save this object to the database and set the ``_id`` field of this
            document to the returned id.
            
            **Parameters**:
                * db: The pymongo database to write to
        '''
        collection = db[self.get_collection_name()]
        for index in self.get_indexes():
            index.ensure(collection)
        id = collection.save(self.wrap())
        self._id = id
    
    def wrap(self):
        '''Returns a transformation of this document into a form suitable to 
            be saved into a mongo database.  This is done by using the ``wrap()``
            methods of the underlying fields to set values.'''
        res = {}
        cls = self.__class__
        for name in dir(cls):
            field = getattr(cls, name)
            try:
                value = getattr(self, name)
            except AttributeError:
                continue
            if isinstance(field, Field):
                res[field.db_field] = field.wrap(value)
        return res
    
    @classmethod
    def unwrap(cls, obj, fields=None):
        '''Returns an instance of this document class based on the mongo object 
            ``obj``.  This is done by using the ``unwrap()`` methods of the 
            underlying fields to set values.
            
            **Parameters**:
                * obj: a ``SON`` object returned from a mongo database
                * fields: A list of :class:`mongoalchemy.query.QueryField` objects \
                    for the fields to load.  If ``None`` is passed all fields  \
                    are loaded
            '''
        
        # Get reverse name mapping
        name_reverse = {}
        for name, field in cls.get_fields().iteritems():
            name_reverse[field.db_field] = name
        
        # Unwrap
        params = {}
        for k, v in obj.iteritems():
            k = name_reverse.get(k, k)
            field = getattr(cls, k)
            if fields != None and isinstance(field, DocumentField):
                normalized_fields = cls.__normalize(fields)
                unwrapped = field.unwrap(v, fields=normalized_fields.get(k))
            else:
                unwrapped = field.unwrap(v)
            params[str(k)] = unwrapped
        
        if fields != None:
            params['retrieved_fields'] = fields
        return cls(**params)

class DocumentField(Field):
    ''' A field which wraps a :class:`Document`'''
    def __init__(self, document_class, **kwargs):
        super(DocumentField, self).__init__(**kwargs)
        self.type = document_class

    def validate_wrap(self, value):
        ''' Called before wrapping.  Calls :func:`~DocumentField.is_valid_wrap` and 
            raises a :class:`BadValueException` if validation fails            
            
            **Parameters**: 
                * value: The value to validate
        '''
        if not self.is_valid_wrap(value):
            self._fail_validation(value)
    
    def validate_unwrap(self, value, fields=None):
        ''' Called before wrapping.  Calls :func:`~DocumentField.is_valid_unwrap` and 
            raises a :class:`BadValueException` if validation fails            
            
            **Parameters**: 
                * value: The value to validate
                * fields: The fields being returned if this is a partial \
                    document. They will be ignored when validating the fields \
                    of ``value``
        '''
        if not self.is_valid_unwrap(value, fields=fields):
            self._fail_validation(value)

    
    def _fail_validation(self, value):
        name = self.__class__.__name__
        raise BadValueException('Bad value for field of type "%s(%s)": %s' %
                        (name, self.type.class_name(), repr(value)))
    
    def wrap(self, value):
        '''Validate ``value`` and then use the document's class to wrap the 
            value'''
        self.validate_wrap(value)
        return self.type.wrap(value)
    
    def unwrap(self, value, fields=None):
        '''Validate ``value`` and then use the document's class to unwrap the 
            value'''
        self.validate_unwrap(value, fields=fields)
        return self.type.unwrap(value, fields=fields)
    
    def is_valid_wrap(self, value):
        ''' Checks that ``value`` is an instance of ``DocumentField.document_class``.
            if it is, then validation on its fields has already been done and
            no further validation is needed.
        '''
        return value.__class__ == self.type
    
    def is_valid_unwrap(self, value, fields=None):
        '''At the moment this method always returns True.  In the future it 
            will go through every field in ``value`` and validate it against 
            the fields in the document class.
            
            .. note::
                Validation will still happen during the actual unwrapping
            '''
        return True

class BadIndexException(Exception):
    pass

class Index(object):
    '''This class is  used in the class definition of a :class:`~Document` to 
        specify a single, possibly compound, index.  ``pymongo``'s ``ensure_index``
        will be called on each index before a database operation is executed 
        on the owner document class.
        
        **Example**
            
            >>> class Donor(Document):
            ...     name = StringField()
            ...     age = IntField(min_value=0)
            ...     blood_type = StringField()
            ...     
            ...     i_name = Index().ascending('name')
            ...     type_age = Index().ascending('blood_type').descending('age')
    '''
    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING
    
    def __init__(self):
        self.components = []
        self.__unique = False
        self.__drop_dups = False
    
    def ascending(self, name):
        '''Add a descending index for ``name`` to this index.
        
        **Parameters**:
            * name: Name to be used in the index
        '''
        self.components.append((name, Index.ASCENDING))
        return self

    def descending(self, name):
        '''Add a descending index for ``name`` to this index.
        
        **Parameters**:
            * name: Name to be used in the index
        '''
        self.components.append((name, Index.DESCENDING))
        return self
    
    def unique(self, drop_dups=False):
        '''Make this index unique, optionally dropping duplicate entries.
                
        **Parameters**:
            * drop_dups: Drop duplicate objects while creating the unique \
                index?  Default to ``False``
        '''
        self.__unique = True
        self.__drop_dups = drop_dups
        return self
    
    def ensure(self, collection):
        ''' Call the pymongo method ``ensure_index`` on the passed collection.
            
            **Parameters**:
                * collection: the ``pymongo`` collection to ensure this index \
                    is on
        '''
        collection.ensure_index(self.components, unique=self.__unique, 
            drop_dups=self.__drop_dups)
        return self
        