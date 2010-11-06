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
from pymongo.objectid import ObjectId
from pymongo.binary import Binary
from mongoalchemy.util import UNSET
import functools
from copy import deepcopy

SCALAR_MODIFIERS = set(['$set', '$unset'])
NUMBER_MODIFIERS = SCALAR_MODIFIERS | set(['$inc'])
LIST_MODIFIERS = SCALAR_MODIFIERS | set(['$push', '$addToSet', '$pull', '$pushAll', '$pullAll', '$pop'])
ANY_MODIFIER = LIST_MODIFIERS | NUMBER_MODIFIERS

class FieldMeta(type):
    def __new__(mcs, classname, bases, class_dict):
        
        def validation_wrapper(fun):
            def wrapped(self, value, *args, **kwds):
                if self._allow_none and value == None:
                    return
                return fun(self, value, *args, **kwds)
            functools.update_wrapper(wrapped, fun, ('__name__', '__doc__'))
            return wrapped
        
        if 'validate_wrap' in class_dict:
            class_dict['validate_wrap'] = validation_wrapper(class_dict['validate_wrap'])
        
        if 'validate_unwrap' in class_dict:
            class_dict['validate_unwrap'] = validation_wrapper(class_dict['validate_unwrap'])
        
        # Create Class
        return type.__new__(mcs, classname, bases, class_dict)

class Field(object):
    auto = False
    
    __metaclass__ = FieldMeta
    
    def __init__(self, required=True, default=UNSET, db_field=None, allow_none=False):
        '''
        **Parameters**:
            * required: The field must be passed when constructing a document (optional. default: ``True``)
            * default:  Default value to use if one is not given (optional.)
            * db_field: name to use when saving or loading this field from the database \
                (optional.  default is the name the field is assigned to on a documet)
            * allow_none: allow ``None`` as a value (optional. default: False)
        '''
        self.required = required
        self._allow_none = allow_none
        self.__db_field = db_field
        if default != UNSET:
            self.default = default
        self.name =  'Unbound_%s' % self.__class__.__name__
    
    @property
    def db_field(self):
        ''' The name to use when setting this field on a document.  If 
            ``db_field`` is passed to the constructor, that is returned.  Otherwise
            the value is the name which this field was assigned to on the owning
            document.
        '''
        if self.__db_field != None:
            return self.__db_field
        return self.name
    
    def _set_name(self, name):
        self.name = name
    
    def _set_parent(self, parent):
        self.parent = parent
    
    def wrap(self, value):
        ''' Returns an object suitable for setting as a value on a MongoDB object.  
            Raises ``NotImplementedError`` in the base class.
            
            **Parameters**: 
                * value: The value to convert.
        '''
        raise NotImplementedError()
    
    def unwrap(self, value):
        ''' Returns an object suitable for setting as a value on a subclass of
            :class:`~mongoalchemy.document.Document`.  
            Raises ``NotImplementedError`` in the base class.
            
            **Parameters**: 
                * value: The value to convert.
            '''
        raise NotImplementedError()
    
    def validate_wrap(self, value):
        ''' Called before wrapping.  Calls :func:`~Field.is_valid_wrap` and 
            raises a :class:`BadValueException` if validation fails            
            
            **Parameters**: 
                * value: The value to validate
        '''
        raise NotImplementedError()
    
    def validate_unwrap(self, value):
        ''' Called before unwrapping.  Calls :func:`~Field.is_valid_unwrap` and raises 
            a :class:`BadValueException` if validation fails
            
            .. note::
                ``is_valid_unwrap`` calls ``is_valid_wrap``, so any class without
                a is_valid_unwrap function is inheriting that behaviour.
        
            **Parameters**: 
                * value: The value to check
        '''
        
        self.validate_wrap(value)
    
    def _fail_validation(self, value, reason='', cause=None):
        raise BadValueException(self.name, value, reason, cause=cause)
    
    def _fail_validation_type(self, value, *type):
        types = '\n'.join([str(t) for t in type])
        got = value.__class__.__name__
        raise BadValueException(self.name, value, 'Value is not an instance of %s (got: %s)' % (types, got))
    
    def is_valid_wrap(self, value):
        '''Returns whether ``value`` is a valid value to wrap.
            Raises ``NotImplementedError`` in the base class.
        
            **Parameters**: 
                * value: The value to check
        '''
        try:
            self.validate_wrap(value)
        except BadValueException:
            return False
        return True
    
    def is_valid_unwrap(self, value):
        ''' Returns whether ``value`` is a valid value to unwrap.
            Raises ``NotImplementedError`` in the base class.
        
            **Parameters**: 
                * value: The value to check
        '''
        try:
            self.validate_unwrap(value)
        except BadValueException:
            return False
        return True

class PrimitiveField(Field):
    '''Primitive fields are fields where a single constructor can be used
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
        '''
        **Parameters**:
            * max_length: maximum string length
            * min_length: minimum string length
            * \*\*kwargs: arguments for :class:`Field`
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
        if not isinstance(value, str) and not isinstance(value, Binary):
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
        '''
        **Parameters**:
            * max_value: maximum value
            * min_value: minimum value
            * \*\*kwargs: arguments for :class:`Field`
        '''
        super(NumberField, self).__init__(constructor=constructor, **kwargs)
        self.min = min_value
        self.max = max_value
        
    def validate_wrap(self, value, type):
        ''' Validates the type and value of ``value`` '''
        if not isinstance(value, type): 
            self._fail_validation_type(value, type)
        if self.min != None and value < self.min:
            self._fail_validation(value, 'Value too small')
        if self.max != None and value > self.max:
            self._fail_validation(value, 'Value too large')

class IntField(NumberField):
    ''' Subclass of :class:`~NumberField` for ``int``'''
    def __init__(self, **kwargs):
        '''
        **Parameters**:
            * max_length: maximum value
            * min_length: minimum value
            * \*\*kwargs: arguments for :class:`Field`
        '''
        super(IntField, self).__init__(constructor=int, **kwargs)
    def validate_wrap(self, value):
        ''' Validates the type and value of ``value`` '''
        NumberField.validate_wrap(self, value, int)

class FloatField(NumberField):
    ''' Subclass of :class:`~NumberField` for ``float`` '''
    def __init__(self, **kwargs):
        '''
        **Parameters**:
            * max_value: maximum value
            * min_value: minimum value
            * \*\*kwargs: arguments for :class:`Field`
        '''
        super(FloatField, self).__init__(constructor=float, **kwargs)
    def validate_wrap(self, value):
        ''' Validates the type and value of ``value`` '''
        return NumberField.validate_wrap(self, value, float)

class DateTimeField(PrimitiveField):
    ''' Field for datetime objects. '''
    def __init__(self, min_date=None, max_date=None, **kwargs):
        '''
        **Parameters**:
            * max_date: maximum date
            * min_date: minimum date
            * \*\*kwargs: arguments for :class:`Field`
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
        '''
            **Parameters**:
                * \*item_types: instances of :class:`Field`, in the order they \
                    will appear in the tuples.
                * \*\*kwargs: arguments for :class:`Field`
        '''
        super(TupleField, self).__init__(**kwargs)
        self.size = len(item_types)
        self.types = item_types
    
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
            **Parameters**
                * value: the tuple (or list) to wrap
        '''
        self.validate_wrap(value)
        ret = []
        for field, value in itertools.izip(self.types, value):
            ret.append(field.wrap(value))
        return ret
    
    def unwrap(self, value):
        ''' Validate and then unwrap ``value`` for object creation.
            **Parameters**
                * value: list returned from the database.  
        '''
        self.validate_unwrap(value)
        ret = []
        for field, value in itertools.izip(self.types, value):
            ret.append(field.unwrap(value))
        return tuple(ret)

class EnumField(Field):
    ''' Represents a single value out of a list of possible values, all 
        of the same type. == is used for comparison
        
        **Example**: ``EnumField(IntField(), 4, 6, 7)`` would accept anything 
        in ``(4, 6, 7)`` as a value.  It would not accept ``5``.
        '''
    
    valid_modifiers = SCALAR_MODIFIERS
    
    def __init__(self, item_type, *values, **kwargs):
        '''
        **Parameters**:
            * item_type: Instance of :class:`Field` to use for validation, and (un)wrapping
            * values: Possible values.  ``item_type.is_valid_wrap(value)`` should be ``True``
        '''
        super(EnumField, self).__init__(**kwargs)
        self.item_type = item_type
        self.values = values
        for value in values:
            self.item_type.validate_wrap(value)
    
    def validate_wrap(self, value):
        ''' Checks that value is valid for `EnumField.item_type` and that 
            value is one of the values specified when the EnumField was 
            constructed '''
        self.item_type.validate_wrap(value)
        
        if value not in self.values:
            self._fail_validation(value, 'Value was not in the enum values')
    
    def validate_unwrap(self, value):
        ''' 
            Checks that value is valid for `EnumField.item_type`.  
            
            .. note ::
                Since checking the value itself is not possible until is is 
                actually unwrapped, that check is done in :func:`EnumField.unwrap`'''
        self.item_type.validate_unwrap(value)
    
    def wrap(self, value):
        '''Validate and wrap value using the wrapping function from ``EnumField.item_type``
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
        '''
            **Parameters**:
                * item_type: :class:`Field` instance used for validation and (un)wrapping
                * min_capacity: minimum number of items contained in values
                * max_capacity: maximum number of items contained in values 
        '''
        super(SequenceField, self).__init__(**kwargs)
        self.item_type = item_type
        self.min = min_capacity
        self.max = max_capacity
        if not isinstance(item_type, Field):
            raise BadFieldSpecification("List item_type is not a field!")
    
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

class ListField(SequenceField):
    '''Field representing a python list.
        
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
    '''Field representing a python set.
        
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
        '''Always returns the value passed in'''
        return value

    def unwrap(self, value):
        '''Always returns the value passed in'''
        return value
    
    def validate_unwrap(self, value):
        '''Always passes'''
        pass
    def validate_wrap(self, value):
        '''Always passes'''
        pass

class ObjectIdField(Field):
    '''pymongo Object ID object.  Currently this is probably too strict.  A 
        string version of an ObjectId should also be acceptable'''
    
    # modifiers on ObjectId not allowed!
    valid_modifiers = set() 
    
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
        '''
            **Parameters**:
                * value_type: the Field type to use for the values
        '''
        super(DictField, self).__init__(**kwargs)
        self.value_type = value_type
        if not isinstance(value_type, Field):
            raise BadFieldSpecification("DictField value type is not a field!")
    
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
        a ``KVField`` is ``[ { 'k' : key, 'v' : value }, ...]``.  This will eventually
        makes it possible to have an index on the keys and values.
    '''
    def __init__(self, key_type, value_type, **kwargs):
        '''
            **Parameters**:
                * key_type: the Field type to use for the keys
                * value_type: the Field type to use for the values
        '''
        super(DictField, self).__init__(**kwargs)
        self.key_type = key_type
        self.value_type = value_type
        if not isinstance(key_type, Field):
            raise BadFieldSpecification("KVField key type is not a field!")
        if not isinstance(value_type, Field):
            raise BadFieldSpecification("KVField value type is not a field!")
    
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
            
            # try:
            #     self.key_type.validate_unwrap(v)
            # except BadValueException, bve:
            #     self._fail_validation(value, 'Bad value for key %s' % k, cause=bve)
            # 
            # try:
            #     self.value_type.validate_unwrap(v)
            # except BadValueException, bve:
            #     self._fail_validation(value, 'Bad value for KVField valye key %s' % k, cause=bve)
            # 
            # 
            # if not self.key_type.is_valid_unwrap(k):
            #     return False
            # if not self.value_type.is_valid_unwrap(v):
            #     return False
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
    ''' A computed field is generated based on an object's other values.  
        
        the unwrap function takes a dictionary of K/V pairs of the 
        dependencies.  Since dependencies are declared in the class 
        definition all of the dependencies for a computed field should be
        in the class definition before the computed field itself.
        
        .. warning::
            The computed field interacts weirdly with documents right now, 
            especially with respect to partial loading.  If using this class
            watch out for strange behaviour
    '''
    
    valid_modifiers = SCALAR_MODIFIERS
    
    auto = True
    def __init__(self, computed_type, deps=None, **kwargs):
        '''
            **Parameters**:
                * fun: the function to compute the value of the computed field
                * computed_type: the type to use when wrapping the computed field
                * deps: the names of fields on the current object which should be \
                    passed in to compute the value
        '''
        super(ComputedField, self).__init__(**kwargs)
        self.computed_type = computed_type
        if deps == None:
            deps = set()
        self.deps = set(deps)
    
    def validate_wrap(self, value):
        '''Check that ``value`` is valid for unwrapping with ``ComputedField.computed_type``'''
        try:
            self.computed_type.validate_wrap(value)
        except BadValueException, bve:
            self._fail_validation(value, 'Bad value for computed field', cause=bve)
    
    def validate_unwrap(self, value):
        '''Check that ``value`` is valid for unwrapping with ``ComputedField.computed_type``'''
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
    
    def __call__(self, fun):
        return ComputedFieldValue(self, fun)


class ComputedFieldValue(property, ComputedField):
    def __init__(self, field, fun):
        self.__computed_value = UNSET
        self.field = field
        self.fun = fun
    
    def compute_value(self, doc):
        args = {}
        for dep in self.field.deps:
            args[dep.name] = getattr(doc, dep.name)
        value = self.fun(args)
        try:
            self.field.computed_type.validate_wrap(value)
        except BadValueException, bve:
            self.field._fail_validation(value, 'Computed Function return a bad value', cause=bve)
        return value
    
    def _set_name(self, name):
        self.name = name
        self.field.name = name
    
    def _set_parent(self, parent):
        self.parent = parent
        self.field.parent = parent
    
    def __set__(self, instance, value):
        if self.field.is_valid_wrap(value):
            self.__computed_value = value
            return
        # TODO: this line should be impossible to reach, but I'd like an 
        # exception just in case, but then I can't have full coverage!
        # raise BadValueException('Tried to set a computed field to an illegal value: %s' % value)
    
    def __get__(self, instance, owner):
        if isinstance(instance, type(None)):
            return self.field
        # TODO: dirty cache indictor + check a field option for never caching
        return self.compute_value(instance)
        # if self.__computed_value == UNSET:
        #     self.__computed_value = self.compute_value(instance)
        # return self.__computed_value

class BadValueException(Exception):
    '''An exception which is raised when there is something wrong with a 
        value'''
    def __init__(self, name, value, reason, cause=None):
        self.name = name
        self.value = value
        self.cause = cause
        Exception.__init__(self, 'Bad value for field of type "%s".  Reason: "%s".  Bad Value: %s\n\n%s' % (name, reason, repr(value), cause))

class BadFieldSpecification(Exception):
    '''An exception that is raised when there is an error in creating a 
        field'''
    pass

