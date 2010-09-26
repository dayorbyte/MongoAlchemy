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

from datetime import datetime
from pymongo.objectid import ObjectId

class BadValueException(Exception):
    pass

class BadFieldSpecification(Exception):
    pass

class Field(object):
    def __init__(self, required=True):
        self.required = required
    
    def set_name(self, name):
        self.name = name
    
    def set_parent(self, parent):
        self.parent = parent
    
    def wrap(self, value):
        raise NotImplementedError()
    
    def unwrap(self, value):
        raise NotImplementedError()
    
    def validate_wrap(self, value):
        if not self.is_valid_wrap(value):
            name = self.__class__.__name__
            raise BadValueException('Bad value for field of type "%s": %s' %
                                    (name, repr(value)))
    def validate_unwrap(self, value):
        if not self.is_valid_unwrap(value):
            name = self.__class__.__name__
            raise BadValueException('Bad value for field of type "%s": %s' %
                                    (name, repr(value)))

    def is_valid_wrap(self, value):
        raise NotImplementedError()
    
    def is_valid_unwrap(self, value):
        return self.is_valid_wrap(value)

class PrimitiveField(Field):
    '''Primitive fields are fields where a single constructor can be used
        for wrapping and unwrapping an object.'''
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
    def __init__(self, max_length=None, min_length=None, **kwargs):
        self.max = max_length
        self.min = min_length
        super(StringField, self).__init__(constructor=unicode, **kwargs)

    def is_valid_wrap(self, value):
        if not isinstance(value, basestring):
            return False
        if self.max != None and len(value) > self.max:
            return False
        if self.min != None and len(value) < self.min:
            return False
        return True

class BoolField(PrimitiveField):
    def __init__(self, **kwargs):
        super(BoolField, self).__init__(constructor=bool, **kwargs)
    def is_valid_wrap(self, value):
        return isinstance(value, bool)

class NumberField(PrimitiveField):
    def __init__(self, constructor, min_value=None, max_value=None, **kwargs):
        super(NumberField, self).__init__(constructor=constructor, **kwargs)
        self.min = min_value
        self.max = max_value

    def is_valid_wrap(self, value, type):
        if not isinstance(value, type): 
            return False
        if self.min != None and value < self.min:
            return False
        if self.max != None and value > self.max:
            return False
        return True

class IntField(NumberField):
    def __init__(self, **kwargs):
        super(IntField, self).__init__(constructor=int, **kwargs)
    def is_valid_wrap(self, value):
        return NumberField.is_valid_wrap(self, value, int)

class FloatField(NumberField):
    def __init__(self, **kwargs):
        super(FloatField, self).__init__(constructor=float, **kwargs)
    def is_valid_wrap(self, value):
        return NumberField.is_valid_wrap(self, value, float)

class DateTimeField(Field):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        super(DateTimeField, self).__init__(**kwargs)
        self.min = min_value
        self.max = max_value
    
    def is_valid_wrap(self, value):
        if not isinstance(value, datetime):
            return False
        if self.min != None and value < self.min:
            return False
        if self.max != None and value > self.max:
            return False
        return True
    
    def wrap(self, value):
        self.validate_wrap(value)
        return value
    unwrap = wrap

class SequenceField(Field):
    def __init__(self, item_type, min_capacity=None, max_capacity=None, **kwargs):
        super(SequenceField, self).__init__(**kwargs)
        self.item_type = item_type
        self.min = min_capacity
        self.max = max_capacity
        if not isinstance(item_type, Field):
            raise BadFieldSpecification("List item_type is not a field!")
    
    def child_type(self):
        return self.item_type
    
    def is_valid_child_wrap(self, value):
        return self.item_type.is_valid_wrap(value)
    
    def is_valid_child_unwrap(self, value):
        return self.item_type.is_valid_unwrap(value)
    
    def length_valid(self, value):
        if self.min != None and len(value) < self.min: 
            return False
        if self.max != None and len(value) > self.max: 
            return False
        return True
    
    def is_valid_wrap(self, value):
        if not self.is_valid_wrap_type(value):
            return False
        if not self.length_valid(value):
            return False
        for v in value:
            if not self.is_valid_child_wrap(v):
                return False
        return True

    def is_valid_unwrap(self, value):
        if not self.is_valid_unwrap_type(value):
            return False
        if not self.length_valid(value):
            return False
        for v in value:
            if not self.is_valid_child_unwrap(v):
                return False
        return True

class ListField(SequenceField):
    def is_valid_wrap_type(self, value):
        return isinstance(value, list) or isinstance(value, tuple)
    is_valid_unwrap_type = is_valid_wrap_type
    
    def wrap(self, value):
        self.validate_wrap(value)
        return [self.item_type.wrap(v) for v in value]
    def unwrap(self, value):
        self.validate_unwrap(value)
        return [self.item_type.unwrap(v) for v in value]

class SetField(SequenceField):
    def is_valid_wrap_type(self, value):
        return isinstance(value, set)
    
    def is_valid_unwrap_type(self, value):
        return isinstance(value, list)
    
    def wrap(self, value):
        self.validate_wrap(value)
        return [self.item_type.wrap(v) for v in value]
    
    def unwrap(self, value):
        self.validate_unwrap(value)
        return set([self.item_type.unwrap(v) for v in value])

class AnythingField(Field):
    def wrap(self, value):
        return value

    def unwrap(self, value):
        return value
    
    def is_valid_wrap(self, value):
        return True

class ObjectIdField(Field):
    '''pymongo Object ID object.  Currently this is probably too strict.  A 
        string version of an ObjectId should also be acceptable'''
    def __init__(self, **kwargs):
        super(ObjectIdField, self).__init__(**kwargs)
    
    def is_valid_wrap(self, value):
        return isinstance(value, ObjectId)
    
    def wrap(self, value):
        self.validate_wrap(value)
        return value
    
    def unwrap(self, value):
        self.validate_unwrap(value)
        return value


class DictField(Field):
    ''' Stores String to <ValueType> Dictionaries.  For non-string keys use 
        KVField.  Strings also must obey the mongo key rules (no . or $)
        '''
    def __init__(self, value_type, **kwargs):
        super(DictField, self).__init__(**kwargs)
        self.value_type = value_type
        if not isinstance(value_type, Field):
            raise BadFieldSpecification("DictField value type is not a field!")
    
    def is_valid_key_wrap(self, key):
        return isinstance(key, basestring) and '.' not in key and '$' not in key
    
    def is_valid_key_unwrap(self, key):
        return self.is_valid_key_wrap(key)
    
    
    def is_valid_unwrap(self, value):
        if not isinstance(value, dict):
            return False
        for k, v in value.iteritems():
            if not self.is_valid_key_unwrap(k):
                return False
            if not self.value_type.is_valid_unwrap(v):
                return False
        return True
        
    def is_valid_wrap(self, value):
        if not isinstance(value, dict):
            return False
        for k, v in value.iteritems():
            if not self.is_valid_key_wrap(k):
                return False
            if not self.value_type.is_valid_wrap(v):
                return False
        return True
    
    def wrap(self, value):
        self.validate_wrap(value)
        ret = {}
        for k, v in value.iteritems():
            ret[k] = self.value_type.wrap(v)
        return ret
    
    def unwrap(self, value):
        self.validate_unwrap(value)
        ret = {}
        for k, v in value.iteritems():
            ret[k] = self.value_type.unwrap(v)
        return ret

class KVField(DictField):
    ''' Like a DictField, except it allows arbitrary keys.  The DB Format for 
        a KVField is { 'k' : <key>, 'v' : <value> }.  This will eventually
        makes it possible to have an index on the keys and values.
    '''
    def __init__(self, key_type, value_type, **kwargs):
        super(DictField, self).__init__(**kwargs)
        self.key_type = key_type
        self.value_type = value_type
        if not isinstance(key_type, Field):
            raise BadFieldSpecification("KVField key type is not a field!")
        if not isinstance(value_type, Field):
            raise BadFieldSpecification("KVField value type is not a field!")
    
    def is_valid_key_wrap(self, key):
        return self.key_type.is_valid_wrap(key)
    
    def is_valid_unwrap(self, value):
        if not isinstance(value, list):
            return False
        for value_dict in value:
            if not isinstance(value_dict, dict):
                return False
            k = value_dict.get('k')
            v = value_dict.get('v')
            if k == None or v == None:
                return False
            if not self.key_type.is_valid_unwrap(k):
                return False
            if not self.value_type.is_valid_unwrap(v):
                return False
        return True
    
    def wrap(self, value):
        self.validate_wrap(value)
        ret = []
        for k, v in value.iteritems():
            ret.append( { 'k' : self.key_type.wrap(k), 'v' : self.value_type.wrap(v) })
        return ret
    
    def unwrap(self, value):
        self.validate_unwrap(value)
        ret = {}
        for value_dict in value:
            k = value_dict['k']
            v = value_dict['v']
            ret[self.key_type.unwrap(k)] = self.value_type.unwrap(v)
        return ret

class ComputedField(Field):
    '''A computed field is generated based on an object's other values.  It
        takes two parameters:

        fun - the function to compute the value of the computed field
        computed_type - the type to use when wrapping the computed field

        the unwrap function takes the whole object instead of a field, and
        should only be called if all of the normal fields are initialized.
        There is NO GUARANTEE that ComputedFields will be evaluated in a
        particular order, so they should not rely on other computed fields.
    '''
    def __init__(self, fun, computed_type, **kwargs):
        super(ComputedField, self).__init__(**kwargs)
        self.fun = fun
        self.computed_type = computed_type
    
    def wrap(self, obj):
        value = self.fun(obj)
        if not self.computed_type.is_valid_wrap(value):
            raise BadValueException('Computed Function return a bad value')
        return self.computed_type.wrap(value)

    def unwrap(self, obj):
        value = self.fun(obj)
        if not self.computed_type.is_valid_wrap(value):
            raise BadValueException('Computed Function return a bad value from the DB')
        return value
        
