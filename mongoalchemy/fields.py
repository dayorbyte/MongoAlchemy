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



import itertools
from datetime import datetime
from bson.objectid import ObjectId
from bson.binary import Binary
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
                if self._allow_none and value == None:
                    return None
                return fun(self, value, *args, **kwds)
            functools.update_wrapper(wrapped, fun, ('__name__', '__doc__'))
            return wrapped
        
        def validation_wrapper(fun, kind):
            def wrapped(self, value, *args, **kwds):
                # Handle None
                if self._allow_none and value == None:
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

class Field(object):
    auto = False
    
    #: If this kind of field can have sub-fields, this attribute should be True
    has_subfields = False
    
    no_real_attributes = False  # used for free-form queries.  
    
    __metaclass__ = FieldMeta
    
    valid_modifiers = SCALAR_MODIFIERS
    
    def __init__(self, required=True, default=UNSET, db_field=None, allow_none=False, on_update='$set', 
            validator=None, unwrap_validator=None, wrap_validator=None, _id=False):
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
        
        self.validator = validator
        self.unwrap_validator = unwrap_validator
        self.wrap_validator = wrap_validator
        
        self._allow_none = allow_none
        self._owner = None
        
        if on_update not in self.valid_modifiers and on_update != 'ignore':
            raise InvalidConfigException('Unsupported update operation: %s' % on_update)
        self.on_update = on_update
        
        self.required = required
        self.default = default
        self._name =  'Unbound_%s' % self.__class__.__name__
    
    def __get__(self, instance, owner):
        if type(instance) == type(None):
            return QueryField(self)
        if self._name in instance._field_values:
            return instance._field_values[self._name]
        if self.default != UNSET:
            return self.default
        if instance.partial and self.db_field not in instance.retrieved_fields:
            raise FieldNotRetrieved(self._name)
            
        raise AttributeError(self._name)
        
    
    def __set__(self, instance, value):
        self.set_value(instance, value)
    
    def __delete__(self, instance):
        if self._name not in instance._field_values:
            raise AttributeError(self._name)
        del instance._field_values[self._name]
        instance._dirty[self._name] = '$unset'
    
    def set_value(self, instance, value, from_db=False):
        instance._field_values[self._name] = value
        if self.on_update != 'ignore':
            instance._dirty[self._name] = self.on_update
    
    def dirty_ops(self, instance):
        op = instance._dirty.get(self._name)
        if op == '$unset':
            return { '$unset' : { self._name : True } }
        if op == None:
            return {}
        return {
            op : {
                self.db_field : self.wrap(instance._field_values[self._name])
            }
        }
    
    def update_ops(self, instance):
        if self._name not in instance._field_values:
            return {}
        return {
            self.on_update : {
                self._name : self.wrap(instance._field_values[self._name])
            }
        }
    
    @property
    def db_field(self):
        ''' The name to use when setting this field on a document.  If 
            ``db_field`` is passed to the constructor, that is returned.  Otherwise
            the value is the name which this field was assigned to on the owning
            document.
        '''
        if self.__db_field != None:
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
    
    def unwrap(self, value):
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

class PrimitiveField(Field):
    ''' Primitive fields are fields where a single constructor can be used
        for wrapping and unwrapping an object.'''
    
    valid_modifiers = SCALAR_MODIFIERS
    
    def __init__(self, constructor, **kwargs):
        super(PrimitiveField, self).__init__(**kwargs)
        self.constructor = constructor
        
    def wrap(self, value):
        self.validate_wrap(value)
        return self.constructor(value)
    def unwrap(self, value):
        self.validate_unwrap(value)
        return self.constructor(value)

class StringField(PrimitiveField):
    ''' Unicode Strings.  ``unicode`` is used to wrap and unwrap values, 
        and any subclass of basestring is an acceptable input'''
    def __init__(self, max_length=None, min_length=None, **kwargs):
        ''' :param max_length: maximum string length
            :param min_length: minimum string length
            :param kwargs: arguments for :class:`Field`
        '''
        self.max = max_length
        self.min = min_length
        super(StringField, self).__init__(constructor=unicode, **kwargs)
        
    def validate_wrap(self, value):
        ''' Validates the type and length of ``value`` '''
        if not isinstance(value, basestring):
            self._fail_validation_type(value, basestring)
        if self.max != None and len(value) > self.max:
            self._fail_validation(value, 'Value too long')
        if self.min != None and len(value) < self.min:
            self._fail_validation(value, 'Value too short')

class BinaryField(PrimitiveField):
    def __init__(self, **kwargs):
        super(BinaryField, self).__init__(constructor=Binary, **kwargs)
    
    def validate_wrap(self, value):
        if not isinstance(value, bytes) and not isinstance(value, Binary):
            self._fail_validation_type(value, str, Binary)

class BoolField(PrimitiveField):
    ''' ``True`` or ``False``.'''
    def __init__(self, **kwargs):
        super(BoolField, self).__init__(constructor=bool, **kwargs)
    def validate_wrap(self, value):
        if not isinstance(value, bool):
            self._fail_validation_type(value, bool)

class NumberField(PrimitiveField):
    ''' Base class for numeric fields '''
    
    valid_modifiers = NUMBER_MODIFIERS
    
    def __init__(self, constructor, min_value=None, max_value=None, **kwargs):
        ''' :param max_value: maximum value
            :param min_value: minimum value
            :param kwargs: arguments for :class:`Field`
        '''
        super(NumberField, self).__init__(constructor=constructor, **kwargs)
        self.min = min_value
        self.max = max_value
        
    def validate_wrap(self, value, *types):
        ''' Validates the type and value of ``value`` '''
        for type in types:
            if isinstance(value, type): 
                break
        else:
            self._fail_validation_type(value, *types)

        if self.min != None and value < self.min:
            self._fail_validation(value, 'Value too small')
        if self.max != None and value > self.max:
            self._fail_validation(value, 'Value too large')

class IntField(NumberField):
    ''' Subclass of :class:`~NumberField` for ``int``'''
    def __init__(self, **kwargs):
        ''' :param max_length: maximum value
            :param min_length: minimum value
            :param kwargs: arguments for :class:`Field`
        '''
        super(IntField, self).__init__(constructor=int, **kwargs)
    def validate_wrap(self, value):
        ''' Validates the type and value of ``value`` '''
        NumberField.validate_wrap(self, value, int)

class FloatField(NumberField):
    ''' Subclass of :class:`~NumberField` for ``float`` '''
    def __init__(self, **kwargs):
        ''' :param max_value: maximum value
            :param min_value: minimum value
            :param kwargs: arguments for :class:`Field`
        '''
        super(FloatField, self).__init__(constructor=float, **kwargs)
    def validate_wrap(self, value):
        ''' Validates the type and value of ``value`` '''
        return NumberField.validate_wrap(self, value, float, int)

class DateTimeField(PrimitiveField):
    ''' Field for datetime objects. '''
    def __init__(self, min_date=None, max_date=None, **kwargs):
        ''' :param max_date: maximum date
            :param min_date: minimum date
            :param kwargs: arguments for :class:`Field`
        '''
        super(DateTimeField, self).__init__(lambda dt : dt, **kwargs)
        self.min = min_date
        self.max = max_date
    
    def validate_wrap(self, value):
        ''' Validates the value's type as well as it being in the valid 
            date range'''
        if not isinstance(value, datetime):
            self._fail_validation_type(value, datetime)
        if self.min != None and value < self.min:
            self._fail_validation(value, 'DateTime too old')
        if self.max != None and value > self.max:
            self._fail_validation(value, 'DateTime too new')

class TupleField(Field):
    ''' Represents a field which is a tuple of a fixed size with specific 
        types for each element in the field.
        
        **Examples** ``TupleField(IntField(), BoolField())`` would accept
        ``[19, False]`` as a value for both wrapping and unwrapping. '''
    
    # uses scalar modifiers since it is not variable length
    valid_modifiers = SCALAR_MODIFIERS
    
    def __init__(self, *item_types, **kwargs):
        ''' :param item_types: instances of :class:`Field`, in the order they \
                    will appear in the tuples.
            :param kwargs: arguments for :class:`Field`
        '''
        super(TupleField, self).__init__(**kwargs)
        self.size = len(item_types)
        self.types = item_types
    
    def set_parent_on_subtypes(self, parent):
        for type in self.types:
            type._set_parent(parent)
    
    def validate_wrap(self, value):
        ''' Checks that the correct number of elements are in ``value`` and that
            each element validates agains the associated Field class
        '''
        if not isinstance(value, list) and not isinstance(value, tuple):
            self._fail_validation_type(value, tuple, list)
        
        for field, value in itertools.izip(self.types, list(value)):
            field.validate_wrap(value)
    
    def validate_unwrap(self, value):
        ''' Checks that the correct number of elements are in ``value`` and that
            each element validates agains the associated Field class
        '''
        if not isinstance(value, list) and not isinstance(value, tuple):
            self._fail_validation_type(value, tuple, list)
        
        for field, value in itertools.izip(self.types, value):
            field.validate_unwrap(value)
    
    def wrap(self, value):
        ''' Validate and then wrap ``value`` for insertion.
            
            :param value: the tuple (or list) to wrap
        '''
        self.validate_wrap(value)
        ret = []
        for field, value in itertools.izip(self.types, value):
            ret.append(field.wrap(value))
        return ret
    
    def unwrap(self, value):
        ''' Validate and then unwrap ``value`` for object creation.
            
            :param value: list returned from the database.  
        '''
        self.validate_unwrap(value)
        ret = []
        for field, value in itertools.izip(self.types, value):
            ret.append(field.unwrap(value))
        return tuple(ret)

class GeoField(TupleField):
    def __init__(self, **kwargs):
        ''' :param item_types: instances of :class:`Field`, in the order they \
                    will appear in the tuples.
            :param kwargs: arguments for :class:`Field`
        '''
        super(GeoField, self).__init__(FloatField(), FloatField(), **kwargs)

class EnumField(Field):
    ''' Represents a single value out of a list of possible values, all 
        of the same type. == is used for comparison
        
        **Example**: ``EnumField(IntField(), 4, 6, 7)`` would accept anything 
        in ``(4, 6, 7)`` as a value.  It would not accept ``5``.
        '''
    
    valid_modifiers = SCALAR_MODIFIERS
    
    def __init__(self, item_type, *values, **kwargs):
        ''' :param item_type: Instance of :class:`Field` to use for validation, and (un)wrapping
            :param values: Possible values.  ``item_type.is_valid_wrap(value)`` should be ``True``
        '''
        super(EnumField, self).__init__(**kwargs)
        self.item_type = item_type
        self.values = values
        # Jan 22, 2011: Commenting this out.  We already check that the value
        # is the right type, and that it is equal to one of the enum values.
        # If those are true, the enum values are the right type.  If we do it
        # now it causes validation issues in some cases with the 
        # string-reference document fields
        #
        # for value in values:
        #     self.item_type.validate_wrap(value)
    
    def set_parent_on_subtypes(self, parent):
        self.item_type._set_parent(parent)
    
    def validate_wrap(self, value):
        ''' Checks that value is valid for `EnumField.item_type` and that 
            value is one of the values specified when the EnumField was 
            constructed '''
        self.item_type.validate_wrap(value)
        
        if value not in self.values:
            self._fail_validation(value, 'Value was not in the enum values')
    
    def validate_unwrap(self, value):
        ''' Checks that value is valid for `EnumField.item_type`.  
            
            .. note ::
                Since checking the value itself is not possible until is is 
                actually unwrapped, that check is done in :func:`EnumField.unwrap`'''
        self.item_type.validate_unwrap(value)
    
    def wrap(self, value):
        ''' Validate and wrap value using the wrapping function from 
            ``EnumField.item_type``
        '''
        self.validate_wrap(value)
        return self.item_type.wrap(value)
    
    def unwrap(self, value):
        ''' Unwrap value using the unwrap function from ``EnumField.item_type``.
            Since unwrap validation could not happen in is_valid_wrap, it 
            happens in this function.'''
        self.validate_unwrap(value)
        value = self.item_type.unwrap(value)
        for val in self.values:
            if val == value:
                return val
        self._fail_validation(value, 'Value was not in the enum values')
    

class SequenceField(Field):
    ''' Base class for Fields which are an iterable collection of objects in which
        every child element is of the same type'''
    
    valid_modifiers = LIST_MODIFIERS
    
    def __init__(self, item_type, min_capacity=None, max_capacity=None, 
            **kwargs):
        ''' :param item_type: :class:`Field` instance used for validation and (un)wrapping
            :param min_capacity: minimum number of items contained in values
            :param max_capacity: maximum number of items contained in values 
        '''
        super(SequenceField, self).__init__(**kwargs)
        self.item_type = item_type
        self.min = min_capacity
        self.max = max_capacity
        if not isinstance(item_type, Field):
            raise BadFieldSpecification("List item_type is not a field!")
    
    @property
    def has_subfields(self):
        ''' Returns True if the sequence's value type has subfields. '''
        return self.item_type.has_subfields
    
    def set_parent_on_subtypes(self, parent):
        self.item_type._set_parent(parent)
    
    def subfields(self):
        ''' Returns the names of the value type's sub-fields'''
        return self.item_type.subfields()
    
    def wrap_value(self, value):
        ''' A function used to wrap a value used in a comparison.  It will 
            first try to wrap as the sequence's sub-type, and then as the 
            sequence itself'''
        try:
            return self.item_type.wrap_value(value)
        except BadValueException:
            pass
        try:
            return self.wrap(value)
        except BadValueException:
            pass
        self._fail_validation(value, 'Could not wrap value as the correct type.  Tried %s and %s' % (self.item_type, self))
    
    def child_type(self):
        ''' Returns the :class:`Field` instance used for items in the sequence'''
        return self.item_type
    
    def _validate_child_wrap(self, value):
        self.item_type.validate_wrap(value)
    
    def _validate_child_unwrap(self, value):
        self.item_type.validate_unwrap(value)
    
    def _length_valid(self, value):
        if self.min != None and len(value) < self.min: 
            self._fail_validation(value, 'Value has too few elements')
        if self.max != None and len(value) > self.max: 
            self._fail_validation(value, 'Value has too many elements')
    
    def validate_wrap(self, value):
        ''' Checks that the type of ``value`` is correct as well as validating
            the elements of value'''
        self._validate_wrap_type(value)
        self._length_valid(value)
        for v in value:
            self._validate_child_wrap(v)
            
    def validate_unwrap(self, value):
        ''' Checks that the type of ``value`` is correct as well as validating
            the elements of value'''
        self._validate_unwrap_type(value)
        self._length_valid(value)
        for v in value:
            self._validate_child_unwrap(v)

    def set_value(self, instance, value, from_db=False):
        super(SequenceField, self).set_value(instance, value, from_db=from_db)

        if from_db:
            # loaded from db, stash it
            if 'orig_values' not in instance.__dict__:
                instance.__dict__['orig_values'] = {}
            instance.__dict__['orig_values'][self._name] = deepcopy(value)

    def dirty_ops(self, instance):
        ops = super(SequenceField, self).dirty_ops(instance)
        if len(ops) == 0:
            # see if the underlying sequence has changed.  Overwrite if so
            try:
                if instance._field_values[self._name] != instance.__dict__['orig_values'][self._name]:
                    ops = {'$set': {
                        self.db_field : self.wrap(instance._field_values[self._name])
                    }}
            except KeyError:
                # required field is missing
                pass
        return ops


class ListField(SequenceField):
    ''' Field representing a python list.
        
        .. seealso:: :class:`SequenceField`'''
    def _validate_wrap_type(self, value):
        if not isinstance(value, list) and not isinstance(value, tuple):
            self._fail_validation_type(value, list, tuple)
    _validate_unwrap_type = _validate_wrap_type
    
    def wrap(self, value):
        ''' Wraps the elements of ``value`` using ``ListField.item_type`` and
            returns them in a list'''
        self.validate_wrap(value)
        return [self.item_type.wrap(v) for v in value]
    def unwrap(self, value):
        ''' Unwraps the elements of ``value`` using ``ListField.item_type`` and
            returns them in a list'''
        self.validate_unwrap(value)
        return [self.item_type.unwrap(v) for v in value]

class SetField(SequenceField):
    ''' Field representing a python set.
        
        .. seealso:: :class:`SequenceField`'''
    def _validate_wrap_type(self, value):
        if not isinstance(value, set):
            self._fail_validation_type(value, set)
    
    def _validate_unwrap_type(self, value):
        if not isinstance(value, list):
            self._fail_validation_type(value, list)
    
    def wrap(self, value):
        ''' Unwraps the elements of ``value`` using ``SetField.item_type`` and
            returns them in a set
            '''
        self.validate_wrap(value)
        return [self.item_type.wrap(v) for v in value]
    
    def unwrap(self, value):
        ''' Unwraps the elements of ``value`` using ``SetField.item_type`` and
            returns them in a set'''
        self.validate_unwrap(value)
        return set([self.item_type.unwrap(v) for v in value])

class AnythingField(Field):
    ''' A field that passes through whatever is set with no validation.  Useful
        for free-form objects '''
    
    valid_modifiers = ANY_MODIFIER
    
    def wrap(self, value):
        ''' Always returns the value passed in'''
        return value
    
    def unwrap(self, value):
        ''' Always returns the value passed in'''
        return value
    
    def validate_unwrap(self, value):
        ''' Always passes'''
        pass
    def validate_wrap(self, value):
        ''' Always passes'''
        pass

class ObjectIdField(Field):
    ''' pymongo Object ID object.  Currently this is probably too strict.  A 
        string version of an ObjectId should also be acceptable'''
    
    valid_modifiers = SCALAR_MODIFIERS 
    
    def __init__(self, **kwargs):
        super(ObjectIdField, self).__init__(**kwargs)
    
    def validate_wrap(self, value):
        ''' Checks that ``value`` is a pymongo ``ObjectId`` or a string 
            representation of one'''
        if not isinstance(value, ObjectId) and not isinstance(value, basestring):
            self._fail_validation_type(value, ObjectId)
        if isinstance(value, ObjectId):
            return
        if len(value) != 24:
            self._fail_validation(value, 'hex object ID is the wrong length')
    
    def wrap(self, value):
        ''' Validates that ``value`` is an ObjectId (or hex representation 
            of one), then returns it '''
        self.validate_wrap(value)
        if isinstance(value, basestring):
            return ObjectId(value)
        return value
    
    def unwrap(self, value):
        ''' Validates that ``value`` is an ObjectId, then returns it '''
        self.validate_unwrap(value)
        return value


class DictField(Field):
    ''' Stores String to ``value_type`` Dictionaries.  For non-string keys use 
        :class:`KVField`.  Strings also must obey the mongo key rules 
        (no ``.`` or ``$``)
        '''
    
    valid_modifiers = SCALAR_MODIFIERS
    
    def __init__(self, value_type, **kwargs):
        ''' :param value_type: the Field type to use for the values
        '''
        super(DictField, self).__init__(**kwargs)
        self.value_type = value_type
        if not isinstance(value_type, Field):
            raise BadFieldSpecification("DictField value type is not a field!")
    
    def set_parent_on_subtypes(self, parent):
        self.value_type._set_parent(parent)
    
    def _validate_key_wrap(self, key):
        if not isinstance(key, basestring):
            self._fail_validation(key, 'DictField keys must be of type basestring')
        if  '.' in key or '$' in key:
            self._fail_validation(key, 'DictField keys cannot contains "." or "$".  You may want a KVField instead')
    
    def _validate_key_unwrap(self, key):
        self._validate_key_wrap(key)
    
    
    def validate_unwrap(self, value):
        ''' Checks that value is a ``dict``, that every key is a valid MongoDB
            key, and that every value validates based on DictField.value_type
        '''
        if not isinstance(value, dict):
            self._fail_validation_type(value, dict)
        for k, v in value.iteritems():
            self._validate_key_unwrap(k)
            try:
                self.value_type.validate_unwrap(v)
            except BadValueException, bve:
                self._fail_validation(value, 'Bad value for key %s' % k, cause=bve)
        
    def validate_wrap(self, value):
        ''' Checks that value is a ``dict``, that every key is a valid MongoDB
            key, and that every value validates based on DictField.value_type
        '''
        if not isinstance(value, dict):
            self._fail_validation_type(value, dict)
        for k, v in value.iteritems():
            self._validate_key_wrap(k)
            try:
                self.value_type.validate_wrap(v)
            except BadValueException, bve:
                self._fail_validation(value, 'Bad value for key %s' % k, cause=bve)
    
    def wrap(self, value):
        ''' Validates ``value`` and then returns a dictionary with each key in
            ``value`` mapped to its value wrapped with ``DictField.value_type``
        '''
        self.validate_wrap(value)
        ret = {}
        for k, v in value.iteritems():
            ret[k] = self.value_type.wrap(v)
        return ret
    
    def unwrap(self, value):
        ''' Validates ``value`` and then returns a dictionary with each key in
            ``value`` mapped to its value unwrapped using ``DictField.value_type``
        '''
        self.validate_unwrap(value)
        ret = {}
        for k, v in value.iteritems():
            ret[k] = self.value_type.unwrap(v)
        return ret

class KVField(DictField):
    ''' Like a DictField, except it allows arbitrary keys.  The DB Format for 
        a ``KVField`` is ``[ { 'k' : key, 'v' : value }, ...]``.  Queries on 
        keys and values. can be done with ``.k`` and ``.v`` '''
    #: If this kind of field can have sub-fields, this attribute should be True
    has_subfields = True
    
    def __init__(self, key_type, value_type, **kwargs):
        ''' :param key_type: the Field type to use for the keys
            :param value_type: the Field type to use for the values
        '''
        super(DictField, self).__init__(**kwargs)
        
        if not isinstance(key_type, Field):
            raise BadFieldSpecification("KVField key type is not a field!")
        if not isinstance(value_type, Field):
            raise BadFieldSpecification("KVField value type is not a field!")
        self.key_type = key_type
        self.key_type._name = 'k'
        
        self.value_type = value_type
        self.value_type._name = 'v'
    
    def set_parent_on_subtypes(self, parent):
        self.value_type._set_parent(parent)
        self.key_type._set_parent(parent)
    
    def subfields(self):
        ''' Returns the k and v subfields, which can be accessed to do queries
            based on either of them
        '''
        return {
            'k' : self.key_type,
            'v' : self.value_type,
        }
    
    def _validate_key_wrap(self, key):
        try:
            self.key_type.validate_wrap(key)
        except BadValueException, bve:
            self._fail_validation(key, 'Bad value for key', cause=bve)
    
    def validate_unwrap(self, value):
        ''' Expects a list of dictionaries with ``k`` and ``v`` set to the 
            keys and values that will be unwrapped into the output python 
            dictionary should have
        '''
        
        if not isinstance(value, list):
            self._fail_validation_type(value, list)
        for value_dict in value:
            if not isinstance(value_dict, dict):
                cause = BadValueException('', value_dict, 'Values in a KVField list must be dicts')
                self._fail_validation(value, 'Values in a KVField list must be dicts', cause=cause)
            k = value_dict.get('k')
            v = value_dict.get('v')
            if k == None:
                self._fail_validation(value, 'Value had None for a key')
            try:
                self.key_type.validate_unwrap(k)
            except BadValueException, bve:
                self._fail_validation(value, 'Bad value for KVField key %s' % k, cause=bve)
            
            try:
                self.value_type.validate_unwrap(v)
            except BadValueException, bve:
                self._fail_validation(value, 'Bad value for KFVield value %s' % k, cause=bve)
        return True
    
    def wrap(self, value):
        ''' Expects a dictionary with the keys being instances of ``KVField.key_type``
            and the values being instances of ``KVField.value_type``.  After validation, 
            the dictionary is transformed into a list of dictionaries with ``k`` and ``v``
            fields set to the keys and values from the original dictionary.
        '''
        self.validate_wrap(value)
        ret = []
        for k, v in value.iteritems():
            k = self.key_type.wrap(k)
            v = self.value_type.wrap(v)
            ret.append( { 'k' : k, 'v' : v })
        return ret
    
    def unwrap(self, value):
        ''' Expects a list of dictionaries with ``k`` and ``v`` set to the 
            keys and values that will be unwrapped into the output python 
            dictionary should have.  Validates the input and then constructs the
            dictionary from the list.
        '''
        self.validate_unwrap(value)
        ret = {}
        for value_dict in value:
            k = value_dict['k']
            v = value_dict['v']
            ret[self.key_type.unwrap(k)] = self.value_type.unwrap(v)
        return ret

class ComputedField(Field):
    ''' A computed field is generated based on an object's other values.  It
        will generally be created with the @computed_field decorator, but
        can be passed an arbitrary function.
        
        The function should take a dict which will contains keys with the names
        of the dependencies mapped to their values.
        
        The computed value is recalculated every the field is accessed unless 
        the one_time field is set to True.
        
        Example::
        
            >>> class SomeDoc(Document):
            ...     @computed_field
            ...     def last_modified(obj):
            ...         return datetime.datetime.utcnow()
        
        
        .. warning::
            The computed field interacts in an undefined way with partially loaded 
            documents right now.  If using this class watch out for strange behaviour.
    '''
    
    valid_modifiers = SCALAR_MODIFIERS
    
    auto = True
    def __init__(self, computed_type, fun, one_time=False, deps=None, **kwargs):
        ''' :param fun: the function to compute the value of the computed field
            :param computed_type: the type to use when wrapping the computed field
            :param deps: the names of fields on the current object which should be \
                passed in to compute the value
        '''
        super(ComputedField, self).__init__(**kwargs)
        self.computed_type = computed_type
        if deps == None:
            deps = set()
        self.deps = set(deps)
        self.fun = fun
        self.one_time = one_time
        self.__cached_value = UNSET
    
    def __get__(self, instance, owner):
        # class method
        if type(instance) == type(None):
            return QueryField(self)
        
        if self._name in instance._field_values and self.one_time:
            return instance._field_values[self._name]
        computed_value = self.compute_value(instance)
        if self.one_time:
            instance._field_values[self._name] = computed_value
        return computed_value
    
    def __set__(self, instance, value):
        if self._name in instance._field_values and self.one_time:
            raise BadValueException(self._name, value, 'Cannot set a one-time field once it has been set')
        super(ComputedField, self).__set__(instance, value)
    
    def set_parent_on_subtypes(self, parent):
        self.computed_type._set_parent(parent)
    
    def dirty_ops(self, instance):
        dirty = False
        for dep in self.deps:
            if dep._name in instance._dirty:
                break
        else:
            if len(self.deps) > 0:
                return {}
        
        return {
            self.on_update : {
                self._name : self.wrap(getattr(instance, self._name))
            }
        }
    
    def compute_value(self, doc):
        args = {}
        for dep in self.deps:
            args[dep._name] = getattr(doc, dep._name)
        value = self.fun(args)
        try:
            self.computed_type.validate_wrap(value)
        except BadValueException, bve:
            self._fail_validation(value, 'Computed Function return a bad value', cause=bve)
        return value
    
    def wrap_value(self, value):
        ''' A function used to wrap a value used in a comparison.  It will 
            first try to wrap as the sequence's sub-type, and then as the 
            sequence itself'''
        return self.computed_type.wrap_value(value)
    
    def validate_wrap(self, value):
        ''' Check that ``value`` is valid for unwrapping with ``ComputedField.computed_type``'''
        try:
            self.computed_type.validate_wrap(value)
        except BadValueException, bve:
            self._fail_validation(value, 'Bad value for computed field', cause=bve)
    
    def validate_unwrap(self, value):
        ''' Check that ``value`` is valid for unwrapping with ``ComputedField.computed_type``'''
        try:
            self.computed_type.validate_unwrap(value)
        except BadValueException, bve:
            self._fail_validation(value, 'Bad value for computed field', cause=bve)
    
    def wrap(self, value):
        ''' Validates ``value`` and wraps it with ``ComputedField.computed_type``'''
        self.validate_wrap(value)
        return self.computed_type.wrap(value)
    
    def unwrap(self, value):
        ''' Validates ``value`` and unwraps it with ``ComputedField.computed_type``'''
        self.validate_unwrap(value)
        return self.computed_type.unwrap(value)

class computed_field(object):
    def __init__(self, computed_type, deps=None, **kwargs):
        self.computed_type = computed_type
        self.deps = deps
        self.kwargs = kwargs
    
    def __call__(self, fun):
        return ComputedField(self.computed_type, fun, deps=self.deps, **self.kwargs)
