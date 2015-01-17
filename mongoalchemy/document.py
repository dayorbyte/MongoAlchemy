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

from __future__ import print_function
from mongoalchemy.py3compat import *

import pymongo
from bson import DBRef
from pymongo import GEO2D
from collections import defaultdict
from mongoalchemy.util import classproperty, UNSET
from mongoalchemy.query_expression import QueryField
from mongoalchemy.fields import (ObjectIdField, Field, BadValueException,
                                 SCALAR_MODIFIERS, DocumentField)
from mongoalchemy.exceptions import DocumentException, MissingValueException, ExtraValueException, FieldNotRetrieved, BadFieldSpecification
from mongoalchemy.util import resolve_name, FieldNotFoundException

document_type_registry = defaultdict(dict)
collection_registry = defaultdict(dict)

class DocumentMeta(type):
    def __new__(mcs, classname, bases, class_dict):
        # Validate Config Options
        # print '-' * 20, classname, '-' * 20

        # Create Class
        class_dict['_subclasses'] = {}
        new_class = type.__new__(mcs, classname, bases, class_dict)

        if new_class.config_extra_fields not in ['error', 'ignore']:
            raise DocumentException("config_extra_fields must be one of: 'error', 'ignore'")

        # 1. Set up links between fields and the document class
        new_id = False
        for name, value in class_dict.items():
            if not isinstance(value, Field):
                continue
            if value.is_id and name != 'mongo_id':
                new_id = True
            value._set_name(name)
            value._set_parent(new_class)

        if new_id:
            new_class.mongo_id = None

        # 2. create a dict of fields to set on the object
        new_class._fields = {}
        for b in bases:
            # print b
            if not hasattr(b, 'get_fields'):
                continue
            for name, field in b.get_fields().items():
                new_class._fields[name] = field

        for name, maybefield in class_dict.items():
            if not isinstance(maybefield, Field):
                continue
            new_class._fields[name] = maybefield

        # 3.  Add subclasses
        for b in bases:
            if 'Document' in globals() and issubclass(b, Document):
                b.add_subclass(new_class)
            if not hasattr(b, 'config_polymorphic_collection'):
                continue
            if b.config_polymorphic_collection and 'config_collection_name' not in class_dict:
                new_class.config_collection_name = b.get_collection_name()

        # 4. register type
        if new_class.config_namespace is not None:
            name = new_class.config_full_name
            if name is None:
                name = new_class.__name__

            ns = new_class.config_namespace
            document_type_registry[ns][name] = new_class

            # if the new class uses a polymorphic collection we should only
            # set up the collection name to refer to the base class
            # TODO: if non-polymorphic classes use the collection registry they
            # will just overwrite for now.
            collection = new_class.get_collection_name()
            current = collection_registry[ns].get(collection)
            if current is None or issubclass(current, new_class):
                collection_registry[ns][collection] = new_class



        return new_class

@add_metaclass(DocumentMeta)
class Document(object):
    # __metaclass__ = DocumentMeta

    mongo_id = ObjectIdField(required=False, db_field='_id', on_update='ignore')
    ''' Default field for the mongo object ID (``_id`` in the database). This field
        is automatically set on objects when they are saved into the database.
        This field can be overridden in subclasses if the default ID is not
        acceptable '''

    config_namespace = 'global'
    ''' The namespace is used to determine how string class names should be
        looked up.  If an instance of DocumentField is created using a string,
        it will be looked up using the value of this variable and the string.
        To have more than one namespace create a subclass of Document
        overriding this class variable.  To turn off caching all together,
        create a subclass where namespace is set to None.  Doing this will
        disable using strings to look up document names, which will make
        creating self-referencing documents impossible.  The default value is
        "global"
    '''

    config_polymorphic = None
    ''' The variable to use when determining which class to instantiate a
        database object with.  It is the name of an attribute which
        will be used to decide the type of the object.  If you want more
        control over which class is selected, you can override
        ``get_subclass``.
    '''

    config_polymorphic_collection = False
    ''' Use the base class collection name for the subclasses.  Default: False
    '''

    config_polymorphic_identity = None
    ''' When using a string value with ``config_polymorphic_on`` in a parent
        class, this is the value that the attribute is compared to when
        determining
    '''

    config_full_name = None
    ''' If namespaces are being used, the key for a class is normally
        the class name.  In some cases the same class name may be used in
        different modules.  This field allows a longer unambiguous name
        to be given.  It may also be used in error messages or string
        representations of the class
    '''

    config_default_sort = None
    ''' The default sort to use when querying.  If set, this sort will be
        applied to any query which a sort isn't used on. The format is the
        same as pymongo.  Example ``[('foo', 1), ('bar', -1)]``.
    '''

    config_extra_fields = 'error'
    ''' Controls the method to use when dealing with fields passed in to the
        document constructor.  Possible values are 'error' and 'ignore'. Any
        fields which couldn't be mapped can be retrieved (and edited) using
        :func:`~Document.get_extra_fields` '''

    def __init__(self, retrieved_fields=None, loading_from_db=False, **kwargs):
        ''' :param retrieved_fields: The names of the fields returned when loading \
                a partial object.  This argument should not be explicitly set \
                by subclasses
            :param \*\*kwargs:  The values for all of the fields in the document. \
                Any additional fields will raise a :class:`~mongoalchemy.document.ExtraValueException` and \
                any missing (but required) fields will raise a :class:`~mongoalchemy.document.MissingValueException`. \
                Both types of exceptions are subclasses of :class:`~mongoalchemy.document.DocumentException`.
        '''
        self.partial = retrieved_fields is not None
        self.retrieved_fields = self.__normalize(retrieved_fields)

        # Mapping from attribute names to values.
        self._values = {}
        self.__extra_fields = {}

        cls = self.__class__

        # Process the fields on the object
        fields = self.get_fields()
        for name, field in fields.items():
            # print name
            if self.partial and field.db_field not in self.retrieved_fields:
                self._values[name] = Value(field, self, retrieved=False)
            elif name in kwargs:
                field = getattr(cls, name)
                value = kwargs[name]
                self._values[name] = Value(field, self,
                                           from_db=loading_from_db)
                field.set_value(self, value)
            elif field.auto:
                self._values[name] = Value(field, self, from_db=False)
            else:
                self._values[name] = Value(field, self, from_db=False)

        # Process any extra fields
        for k in kwargs:
            if k not in fields:
                if self.config_extra_fields == 'ignore':
                    self.__extra_fields[k] = kwargs[k]
                else:
                    raise ExtraValueException(k)

        self.__extra_fields_orig = dict(self.__extra_fields)

        # Validate defult sort
        if self.config_default_sort:
            for (name, direction) in self.config_default_sort:
                try:
                    resolve_name(type(self), name)
                    dirs = (1, -1, pymongo.ASCENDING, pymongo.DESCENDING)
                    if direction not in dirs:
                        m = 'Bad sort direction on %s: %s' % (name, direction)
                        raise BadFieldSpecification(m)
                except FieldNotFoundException:
                    raise BadFieldSpecification("Could not resolve field %s in"
                            " config_default_sort" % name)


    @classmethod
    def schema_json(cls):
        ret = dict(fields={},
                   config_namespace=cls.config_namespace,
                   config_polymorphic=cls.config_polymorphic,
                   config_polymorphic_collection=cls.config_polymorphic_collection,
                   config_polymorphic_identity=cls.config_polymorphic_identity,
                   config_full_name=cls.config_full_name,
                   config_extra_fields=cls.config_extra_fields)
        for f in cls.get_fields():
            ret['fields'][f] = getattr(cls, f).schema_json()
        return ret

    def __deepcopy__(self, memo):
        return type(self).unwrap(self.wrap(), session=self._get_session())

    @classmethod
    def add_subclass(cls, subclass):
        ''' Register a subclass of this class.  Maps the subclass to the
            value of subclass.config_polymorphic_identity if available.
        '''
        #
        import inspect
        for superclass in inspect.getmro(cls)[1:]:
            if issubclass(superclass, Document):
                superclass.add_subclass(subclass)

        # if not polymorphic, stop
        if hasattr(subclass, 'config_polymorphic_identity'):
            attr = subclass.config_polymorphic_identity
            cls._subclasses[attr] = subclass

    @classmethod
    def base_query(cls, exclude_subclasses=False):
        ''' Return the base query for this kind of document. If this class is
            not polymorphic, the query is empty. If it is polymorphic then
            a filter is added to match only this class and its subclasses.

            :param exclude_subclasses: If this is true, only match the current \
                class. If it is false, the default, also return subclasses of \
                this class.
        '''
        if not cls.config_polymorphic:
            return {}
        if exclude_subclasses:
            if cls.config_polymorphic_identity:
                return { cls.config_polymorphic : cls.config_polymorphic_identity }
            return {}
        keys = [key for key in cls._subclasses]
        if cls.config_polymorphic_identity:
            keys.append(cls.config_polymorphic_identity)
        return {
            cls.config_polymorphic : {
                '$in' : keys
            }
        }

    @classmethod
    def get_subclass(cls, obj):
        ''' Get the subclass to use when instantiating a polymorphic object.
            The default implementation looks at ``cls.config_polymorphic``
            to get the name of an attribute.  Subclasses automatically
            register their value for that attribute on creation via their
            ``config_polymorphic_identity`` field.  This process is then
            repeated recursively until None is returned (indicating that the
            current class is the correct one)

            This method can be overridden to allow any method you would like
            to use to select subclasses. It should return either the subclass
            to use or None, if the original class should be used.
        '''
        if cls.config_polymorphic is None:
            return

        value = obj.get(cls.config_polymorphic)
        value = cls._subclasses.get(value)
        if value == cls or value is None:
            return None

        sub_value = value.get_subclass(obj)
        if sub_value is None:
            return value
        return sub_value

    def __eq__(self, other):
        try:
            return self.mongo_id == other.mongo_id
        except:
            return False
    def __ne__(self, other):
        return not self.__eq__(other)

    def get_dirty_ops(self, with_required=False):
        ''' Returns a dict with the update operations necessary to make the
            changes to this object to the database version.  It is mainly used
            internally for :func:`~mongoalchemy.session.Session.update` but
            may be useful for diagnostic purposes as well.

            :param with_required: Also include any field which is required.  This \
                is useful if the method is being called for the purposes of \
                an upsert where all required fields must always be sent.
        '''
        update_expression = {}
        for name, field in self.get_fields().items():
            if field.db_field == '_id':
                continue
            dirty_ops = field.dirty_ops(self)
            if not dirty_ops and with_required and field.required:
                dirty_ops = field.update_ops(self, force=True)
                if not dirty_ops:
                    raise MissingValueException(name)

            for op, values in dirty_ops.items():
                update_expression.setdefault(op, {})
                for key, value in values.items():
                    update_expression[op][key] = value

        if self.config_extra_fields == 'ignore':
            old_extrakeys = set(self.__extra_fields_orig.keys())
            cur_extrakeys = set(self.__extra_fields.keys())

            new_extrakeys = cur_extrakeys - old_extrakeys
            rem_extrakeys = old_extrakeys - cur_extrakeys
            same_extrakeys = cur_extrakeys & old_extrakeys

            update_expression.setdefault('$unset', {})
            for key in rem_extrakeys:
                update_expression['$unset'][key] = True

            update_expression.setdefault('$set', {})
            for key in new_extrakeys:
                update_expression['$set'][key] = self.__extra_fields[key]

            for key in same_extrakeys:
                if self.__extra_fields[key] != self.__extra_fields_orig[key]:
                    update_expression['$set'][key] = self.__extra_fields[key]

        return update_expression

    def get_extra_fields(self):
        ''' if :attr:`Document.config_extra_fields` is set to 'ignore', this method will return
            a dictionary of the fields which couldn't be mapped to the document.
        '''
        return self.__extra_fields

    @classmethod
    def get_fields(cls):
        ''' Returns a dict mapping the names of the fields in a document
            or subclass to the associated :class:`~mongoalchemy.fields.Field`
        '''
        return cls._fields

    @classmethod
    def class_name(cls):
        ''' Returns the name of the class. The name of the class is also the
            default collection name.

            .. seealso:: :func:`~Document.get_collection_name`
        '''
        return cls.__name__

    @classmethod
    def get_collection_name(cls):
        ''' Returns the collection name used by the class.  If the ``config_collection_name``
            attribute is set it is used, otherwise the name of the class is used.'''
        if not hasattr(cls, 'config_collection_name'):
            return cls.__name__
        return cls.config_collection_name

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
    def transform_incoming(self, obj, session):
        """ Tranform the SON object into one which will be able to be
            unwrapped by this document class.

            This method is designed for schema migration systems.
        """
        return obj


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

    def has_id(self):
        try:
            getattr(self, 'mongo_id')
        except AttributeError:
            return False
        return True
    def to_ref(self, db=None):
        return DBRef(id=self.mongo_id,
                     collection=self.get_collection_name(),
                     database=db)
    def wrap(self):
        ''' Returns a transformation of this document into a form suitable to
            be saved into a mongo database.  This is done by using the ``wrap()``
            methods of the underlying fields to set values.'''
        res = {}
        for k, v in self.__extra_fields.items():
            res[k] = v
        cls = self.__class__
        for name in self.get_fields():
            field = getattr(cls, name)
            try:
                value = getattr(self, name)
                res[field.db_field] = field.wrap(value)
            except AttributeError as e:
                if field.required:
                    raise MissingValueException(name)
            except FieldNotRetrieved as fne:
                if field.required:
                    raise
        return res

    @classmethod
    def unwrap(cls, obj, fields=None, session=None):
        ''' Returns an instance of this document class based on the mongo object
            ``obj``.  This is done by using the ``unwrap()`` methods of the
            underlying fields to set values.

            :param obj: a ``SON`` object returned from a mongo database
            :param fields: A list of :class:`mongoalchemy.query.QueryField` objects \
                    for the fields to load.  If ``None`` is passed all fields  \
                    are loaded
            '''

        subclass = cls.get_subclass(obj)
        if subclass and subclass != cls:
            unwrapped = subclass.unwrap(obj, fields=fields, session=session)
            unwrapped._session = session
            return unwrapped
        # Get reverse name mapping
        name_reverse = {}
        for name, field in cls.get_fields().items():
            name_reverse[field.db_field] = name
        # Unwrap
        params = {}
        for k, v in obj.items():
            k = name_reverse.get(k, k)
            if not hasattr(cls, k) and cls.config_extra_fields:
                params[str(k)] = v
                continue

            field = getattr(cls, k)
            field_is_doc = fields is not None and isinstance(field.get_type(), DocumentField)

            extra_unwrap = {}
            if field.has_autoload:
                extra_unwrap['session'] = session
            if field_is_doc:
                normalized_fields = cls.__normalize(fields)
                unwrapped = field.unwrap(v, fields=normalized_fields.get(k), **extra_unwrap)
            else:
                unwrapped = field.unwrap(v, **extra_unwrap)
            unwrapped = field.localize(session, unwrapped)
            params[str(k)] = unwrapped

        if fields is not None:
            params['retrieved_fields'] = fields
        obj = cls(loading_from_db=True, **params)
        obj._mark_clean()
        obj._session = session
        return obj

    _session = None
    def _get_session(self):
        return self._session
    def _set_session(self, session):
        self._session = session

    def _mark_clean(self):
        for k, v in self._values.items():
            v.clear_dirty()


class DictDoc(object):
    ''' Adds a mapping interface to a document.  Supports ``__getitem__`` and
        ``__contains__``.  Both methods will only retrieve values assigned to
        a field, not methods or other attributes.
    '''
    def __getitem__(self, name):
        ''' Gets the field ``name`` from the document '''
        # fields = self.get_fields()
        if name in self._values:
            return getattr(self, name)
        raise KeyError(name)

    def __setitem__(self, name, value):
        ''' Sets the field ``name`` on the document '''
        setattr(self, name, value)

    def setdefault(self, name, value):
        ''' if the ``name`` is set, return its value.  Otherwse set ``name`` to
            ``value`` and return ``value``'''
        if name in self:
            return self[name]
        self[name] = value
        return self[name]

    def __contains__(self, name):
        ''' Return whether a field is present.  Fails if ``name`` is not a
            field or ``name`` is not set on the document or if ``name`` was
            not a field retrieved from the database
        '''
        try:
            self[name]
        except FieldNotRetrieved:
            return False
        except AttributeError:
            return False
        except KeyError:
            return False
        return True


class BadIndexException(Exception):
    pass

class Index(object):
    ''' This class is  used in the class definition of a :class:`~Document` to
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
        self.__min = None
        self.__max = None
        self.__bucket_size = None
        self.__expire_after = None

    def expire(self, after):
        '''Add an expire after option to the index

           :param: after: Number of second before expiration

        '''
        self.__expire_after = after
        return self


    def ascending(self, name):
        ''' Add a descending index for ``name`` to this index.

            :param name: Name to be used in the index
        '''
        self.components.append((name, Index.ASCENDING))
        return self

    def descending(self, name):
        ''' Add a descending index for ``name`` to this index.

            :param name: Name to be used in the index
        '''
        self.components.append((name, Index.DESCENDING))
        return self

    def geo2d(self, name, min=None, max=None):
        """ Create a 2d index.  See:
            http://www.mongodb.org/display/DOCS/Geospatial+Indexing

            :param name: Name of the indexed column
            :param min: minimum value for the index
            :param max: minimum value for the index
        """
        self.components.append((name, GEO2D))
        self.__min = min
        self.__max = max
        return self

    def geo_haystack(self, name, bucket_size):
        """ Create a Haystack index.  See:
            http://www.mongodb.org/display/DOCS/Geospatial+Haystack+Indexing

            :param name: Name of the indexed column
            :param bucket_size: Size of the haystack buckets (see mongo docs)
        """
        self.components.append((name, 'geoHaystack'))
        self.__bucket_size = bucket_size
        return self

    def unique(self, drop_dups=False):
        ''' Make this index unique, optionally dropping duplicate entries.

            :param drop_dups: Drop duplicate objects while creating the unique \
                index?  Default to ``False``
        '''
        self.__unique = True
        self.__drop_dups = drop_dups
        return self

    def ensure(self, collection):
        ''' Call the pymongo method ``ensure_index`` on the passed collection.

            :param collection: the ``pymongo`` collection to ensure this index \
                    is on
        '''
        components = []
        for c in self.components:
            if isinstance(c[0], Field):
                c = (c[0].db_field, c[1])
            components.append(c)

        extras = {}
        if self.__min is not None:
            extras['min'] = self.__min
        if self.__max is not None:
            extras['max'] = self.__max
        if self.__bucket_size is not None:
            extras['bucket_size'] = self.__bucket_size
        if self.__expire_after is not None:
            extras['expireAfterSeconds'] = self.__expire_after

        collection.ensure_index(components, unique=self.__unique,
            drop_dups=self.__drop_dups, **extras)
        return self

class Value(object):
    def __init__(self, field, document, from_db=False, extra=False,
                 retrieved=True):
        # Stuff
        self.field = field
        self.doc = document
        self.value = None

        # Flags
        self.from_db = from_db
        self.set = False
        self.extra = extra
        self.dirty = False
        self.retrieved = retrieved
        self.update_op = None
    def clear_dirty(self):
        self.dirty = False
        self.update_op = None

    def delete(self):
        self.value = None
        self.set = False
        self.dirty = True
        self.from_db = False
        self.update_op = '$unset'

