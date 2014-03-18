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

:class:`Field` objects transform python objects into objects which can
be stored as a value in a MongoDB document.  They control the conversions and
validation of the data.

If you want to define your own type of :class:`Field` there are four methods
a subclass must implement:

* :func:`Field.wrap` --- Takes a value and returns an object composed entirely \
    of types that MongoDB understands (dicts, lists, numbers, strings, datetimes, etc.)
* :func:`Field.unwrap` --- Takes a value in the format produced by :func:`~Field.wrap` and
    returns a python object.

:func:`~Field.wrap` and :func:`~Field.unwrap` should be inverse operations.
In particular, ``field.unwrap(field.wrap(obj))`` == obj should always be true.

* :func:`Field.validate_wrap` --- Raises a :class:`BadValueException` if calling :func:`~Field.wrap` will \
    fail.  This function should be fast, as it will be called whenever a value
    is set on a document for this type of field.
* :func:`Field.validate_unwrap` --- Raises a :class:`BadValueException` if calling :func:`~Field.unwrap` will \
    fail.

To just check whether something is valid for wrapping or unwrapping, each field has a
:func:`Field.is_valid_wrap` and an :func:`Field.is_valid_unwrap` function which call
their respective validation function, returning True if a
:class:`BadValueException` is not raised.


The documentation for each :class:`Field` class will largely just be giving the input and
output types for :func:`~Field.wrap` and :func:`~Field.unwrap`.

'''

from __future__ import print_function
from mongoalchemy.py3compat import *
import itertools
from datetime import datetime
from bson.objectid import ObjectId
from bson.binary import Binary
from bson.dbref import DBRef
import functools
from copy import deepcopy

from mongoalchemy.util import UNSET
from mongoalchemy.query_expression import QueryField
from mongoalchemy.exceptions import BadValueException, FieldNotRetrieved, InvalidConfigException, BadFieldSpecification, MissingValueException

SCALAR_MODIFIERS = set(['$set', '$unset'])
NUMBER_MODIFIERS = SCALAR_MODIFIERS | set(['$inc'])
LIST_MODIFIERS = SCALAR_MODIFIERS | set(['$push', '$addToSet', '$pull', '$pushAll', '$pullAll', '$pop'])
ANY_MODIFIER = LIST_MODIFIERS | NUMBER_MODIFIERS

class FieldMeta(type):
    def __new__(mcs, classname, bases, class_dict):

        def wrap_unwrap_wrapper(fun):
            def wrapped(self, value, *args, **kwds):
                if self._allow_none and value is None:
                    return None
                return fun(self, value, *args, **kwds)
            functools.update_wrapper(wrapped, fun, ('__name__', '__doc__'))
            return wrapped

        def validation_wrapper(fun, kind):
            def wrapped(self, value, *args, **kwds):
                # Handle None
                if self._allow_none and value is None:
                    return
                # Standard Field validation
                fun(self, value, *args, **kwds)

                # Universal user-supplied validator
                if self.validator:
                    if self.validator(value) == False:
                        self._fail_validation(value, 'user-supplied validator failed')

                if kind == 'unwrap' and self.unwrap_validator:
                    if self.unwrap_validator(value) == False:
                        self._fail_validation(value, 'user-supplied unwrap_validator failed')

                elif kind == 'wrap' and self.wrap_validator:
                    if self.wrap_validator(value) == False:
                        self._fail_validation(value, 'user-supplied wrap_validator failed')

            functools.update_wrapper(wrapped, fun, ('__name__', '__doc__'))
            return wrapped

        if 'wrap' in class_dict:
            class_dict['wrap'] = wrap_unwrap_wrapper(class_dict['wrap'])
        if 'unwrap' in class_dict:
            class_dict['unwrap'] = wrap_unwrap_wrapper(class_dict['unwrap'])

        if 'validate_wrap' in class_dict:
            class_dict['validate_wrap'] = validation_wrapper(class_dict['validate_wrap'], 'wrap')

        if 'validate_unwrap' in class_dict:
            class_dict['validate_unwrap'] = validation_wrapper(class_dict['validate_unwrap'], 'unwrap')

        # Create Class
        return type.__new__(mcs, classname, bases, class_dict)

@add_metaclass(FieldMeta)
class Field(object):
    auto = False

    #: If this kind of field can have sub-fields, this attribute should be True
    has_subfields = False

    #: If this kind of field can do extra requests, this attribute should be True
    has_autoload = False

    #: Is this a sequence?  used by elemMatch
    is_sequence_field = False

    no_real_attributes = False  # used for free-form queries.

    __metaclass__ = FieldMeta

    valid_modifiers = SCALAR_MODIFIERS

    def __init__(self, required=True, default=UNSET, default_f=None,
                 db_field=None, allow_none=False, on_update='$set',
                 validator=None, unwrap_validator=None, wrap_validator=None,
                 _id=False, proxy=None, iproxy=None, ignore_missing=False):
        '''
            :param required: The field must be passed when constructing a document (optional. default: ``True``)
            :param default:  Default value to use if one is not given (optional.)
            :param db_field: name to use when saving or loading this field from the database \
                (optional.  default is the name the field is assigned to on a documet)
            :param allow_none: allow ``None`` as a value (optional. default: False)
            :param validator: a callable which will be called on objects when wrapping/unwrapping
            :param unwrap_validator: a callable which will be called on objects when unwrapping
            :param wrap_validator: a callable which will be called on objects when wrapping
            :param _id: Set the db_field to _id.  If a field has this the "mongo_id" field will \
                also be removed from the document the field is on.

            The general validator is called after the field's validator, but before
            either of the wrap/unwrap versions.  The validator should raise a BadValueException
            if it fails, but if it returns False the field will raise an exception with
            a generic message.

        '''

        if _id and db_field is not None:
            raise InvalidConfigException('Cannot set db_field and _id on the same Field')
        if _id:
            self.__db_field = '_id'
        else:
            self.__db_field = db_field
        self.is_id = self.__db_field == '_id'
        self.__value = UNSET
        self.__update_op = UNSET

        self.proxy = proxy
        self.iproxy = iproxy
        self.ignore_missing = ignore_missing

        self.validator = validator
        self.unwrap_validator = unwrap_validator
        self.wrap_validator = wrap_validator

        self._allow_none = allow_none

        self.required = required
        self._default = default
        self._default_f = default_f
        if self._default_f and self._default != UNSET:
            raise InvalidConfigException('Only one of default and default_f '
                                         'is allowed')

        if default is None:
            self._allow_none = True
        self._owner = None

        if on_update not in self.valid_modifiers and on_update != 'ignore':
            raise InvalidConfigException('Unsupported update operation: %s'
                                         % on_update)
        self.on_update = on_update

        self._name =  'Unbound_%s' % self.__class__.__name__

    @property
    def default(self):
        if self._default_f:
            return self._default_f()
        return self._default

    def schema_json(self):
        schema = dict(
            type=type(self).__name__,
            required=self.required,
            db_field=self.__db_field,
            allow_none=self._allow_none,
            on_update=self.on_update,
            validator_set=self.validator is not None,
            unwrap_validator=self.unwrap_validator is not None,
            wrap_validator=self.wrap_validator is not None,
            ignore_missing=self.ignore_missing,
        )
        if self._default == UNSET and self._default_f is None:
            schema['default_unset'] = True
        elif self._default_f:
            schema['default_f'] = repr(self._default_f)
        else:
            schema['default'] = self.wrap(self._default)
        return schema


    def __get__(self, instance, owner):
        if instance is None:
            return QueryField(self)
        obj_value = instance._values[self._name]

        # if the value is set, just return it
        if obj_value.set:
            return instance._values[self._name].value

        # if not, try the default
        if self._default_f:
            self.set_value(instance, self._default_f())
            return instance._values[self._name].value
        elif self._default is not UNSET:
            self.set_value(instance, self._default)
            return instance._values[self._name].value

        # If this value wasn't retrieved, raise a specific exception
        if not obj_value.retrieved:
            raise FieldNotRetrieved(self._name)

        raise AttributeError(self._name)


    def __set__(self, instance, value):
        self.set_value(instance, value)

    def set_value(self, instance, value):
        self.validate_wrap(value)
        obj_value = instance._values[self._name]
        obj_value.value = value
        obj_value.dirty = True
        obj_value.set = True
        obj_value.from_db = False
        if self.on_update != 'ignore':
            obj_value.update_op = self.on_update

    def dirty_ops(self, instance):
        obj_value = instance._values[self._name]
        # op = instance._dirty.get(self._name)
        if obj_value.update_op == '$unset':
            return { '$unset' : { self._name : True } }
        if obj_value.update_op is None:
            return {}
        return {
            obj_value.update_op : {
                self.db_field : self.wrap(obj_value.value)
            }
        }

    def __delete__(self, instance):
        obj_value = instance._values[self._name]
        if not obj_value.set:
            raise AttributeError(self._name)
        obj_value.delete()
        # if self._name not in instance._field_values:
        #     raise AttributeError(self._name)
        # del instance._field_values[self._name]
        # instance._dirty[self._name] = '$unset'

    def update_ops(self, instance, force=False):
        obj_value = instance._values[self._name]
        if obj_value.set and (obj_value.dirty or force):
            return {
                self.on_update : {
                    self._name : self.wrap(obj_value.value)
                }
            }
        return {}

    def localize(self, session, value):
        return value

    @property
    def db_field(self):
        ''' The name to use when setting this field on a document.  If
            ``db_field`` is passed to the constructor, that is returned.  Otherwise
            the value is the name which this field was assigned to on the owning
            document.
        '''
        if self.__db_field is not None:
            return self.__db_field
        return self._name

    def wrap_value(self, value):
        ''' Wrap ``value`` for use as the value in a Mongo query, for example
            in $in'''
        return self.wrap(value)

    def _set_name(self, name):
        self._name = name

    def _set_parent(self, parent):
        self.parent = parent
        self.set_parent_on_subtypes(parent)

    def set_parent_on_subtypes(self, parent):
        ''' This function sets the parent on any sub-Fields of this field. It
            should be overridden by SequenceField and field which has subtypes
            (such as SequenceField and DictField).
        '''
        pass

    def wrap(self, value):
        ''' Returns an object suitable for setting as a value on a MongoDB object.
            Raises ``NotImplementedError`` in the base class.

            :param value: The value to convert.
        '''
        raise NotImplementedError()

    def unwrap(self, value, session=None):
        ''' Returns an object suitable for setting as a value on a subclass of
            :class:`~mongoalchemy.document.Document`.
            Raises ``NotImplementedError`` in the base class.

            :param value: The value to convert.
            '''
        raise NotImplementedError()

    def validate_wrap(self, value):
        ''' Called before wrapping.  Calls :func:`~Field.is_valid_wrap` and
            raises a :class:`BadValueException` if validation fails

            :param value: The value to validate
        '''
        raise NotImplementedError()

    def validate_unwrap(self, value):
        ''' Called before unwrapping.  Calls :func:`~Field.is_valid_unwrap` and raises
            a :class:`BadValueException` if validation fails

            .. note::
                ``is_valid_unwrap`` calls ``is_valid_wrap``, so any class without
                a is_valid_unwrap function is inheriting that behaviour.

            :param value: The value to check
        '''

        self.validate_wrap(value)

    def _fail_validation(self, value, reason='', cause=None):
        raise BadValueException(self._name, value, reason, cause=cause)

    def _fail_validation_type(self, value, *type):
        types = '\n'.join([str(t) for t in type])
        got = value.__class__.__name__
        raise BadValueException(self._name, value, 'Value is not an instance of %s (got: %s)' % (types, got))

    def is_valid_wrap(self, value):
        ''' Returns whether ``value`` is a valid value to wrap.
            Raises ``NotImplementedError`` in the base class.

            :param value: The value to check
        '''
        try:
            self.validate_wrap(value)
        except BadValueException:
            return False
        return True

    def is_valid_unwrap(self, value):
        ''' Returns whether ``value`` is a valid value to unwrap.
            Raises ``NotImplementedError`` in the base class.

            :param value: The value to check
        '''
        try:
            self.validate_unwrap(value)
        except BadValueException:
            return False
        return True

