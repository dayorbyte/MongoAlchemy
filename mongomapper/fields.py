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

class Field(object):
    def __init__(self, required=True):
        self.required = required
    
    def is_valid_child(self, value):
        '''Only Used for container-like fields.  Can't have a child for 
            anything else'''
        return False
    
    def set_name(self, name):
        self.name = name
    
    def set_parent(self, parent):
        self.parent = parent
    
    def wrap(self, value):
        raise NotImplemented
    
    def unwrap(self, value):
        raise NotImplemented
    
    def validate(self, value):
        if not self.is_valid(value):
            name = self.__class__.__name__
            raise BadValueException('Bad value for field of type "%s": %s' %
                                    (name, repr(value)))

    def is_valid(self, value):
        raise NotImplemented

class PrimitiveField(Field):
    '''Primitive fields are fields where a single constructor can be used
        for wrapping and unwrapping an object.'''
    def __init__(self, constructor, **kwargs):
        super(PrimitiveField, self).__init__(**kwargs)
        self.constructor = constructor

    def wrap(self, value):
        self.validate(value)
        return self.constructor(value)
    unwrap = wrap

class StringField(PrimitiveField):
    def __init__(self, max_length=None, min_length=None, **kwargs):
        self.max = max_length
        self.min = min_length
        super(StringField, self).__init__(constructor=unicode, **kwargs)

    def is_valid(self, value):
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
    def is_valid(self, value):
        return isinstance(value, bool)

class NumberField(PrimitiveField):
    def __init__(self, constructor, min_value=None, max_value=None, **kwargs):
        super(NumberField, self).__init__(constructor=constructor, **kwargs)
        self.min = min_value
        self.max = max_value

    def is_valid(self, value, type):
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
    def is_valid(self, value):
        return NumberField.is_valid(self, value, int)

class FloatField(NumberField):
    def __init__(self, **kwargs):
        super(FloatField, self).__init__(constructor=float, **kwargs)
    def is_valid(self, value):
        return NumberField.is_valid(self, value, float)

class DateTimeField(Field):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        super(DateTimeField, self).__init__(**kwargs)
        self.min = min_value
        self.max = max_value
    
    def is_valid(self, value):
        if not isinstance(value, datetime):
            return False
        if self.min != None and value < self.min:
            return False
        if self.max != None and value > self.max:
            return False
        return True
    
    def wrap(self, value):
        self.validate(value)
        return value
    unwrap = wrap

class ListField(Field):
    def __init__(self, item_type, **kwargs):
        super(ListField, self).__init__(**kwargs)
        self.item_type = item_type
        if not isinstance(item_type, Field):
            raise Exception("List item_type is not a field!")
    
    def is_valid_child(self, value):
        return self.item_type.is_valid(value)
    
    def is_valid(self, value):
        if not isinstance(value, list): 
            return False
        if self.min != None and len(value) < self.min: 
            return False
        if self.max != None and len(value) > self.max: 
            return False
        return True
    
    def wrap(self, value):
        self.validate(value)
        return value
    unwrap = wrap
    

class SetField(PrimitiveField):
    def __init__(self, item_type, min_capacity=None, max_capacity=None, **kwargs):
        Field.__init__(self, **kwargs)
        self.min = min_capacity
        self.max = max_capacity
        self.item_type = item_type
        if not isinstance(item_type, Field):
            raise Exception("SetField item_type is not a field!")
    
    def is_valid(self, value):
        if not isinstance(value, set): return False
        if self.min != None and len(value) < self.min: return False
        if self.max != None and len(value) > self.max: return False
        return True
    
    def wrap(self, value):
        return [self.item_type.wrap(v) for v in value]

    def unwrap(self, value):
        return set([self.item_type.unwrap(v) for v in value])

class AnythingField(Field):
    def wrap(self, value):
        return value

    def unwrap(self, value):
        return value

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
        if not self.computed_type.is_valid(value):
            raise Exception('Computed Function return a bad value')
        return self.computed_type.wrap(value)

    def unwrap(self, obj):
        return self.fun(obj)

class ObjectIdField(Field):
    def __init__(self, **kwargs):
        super(ObjectIdField, self).__init__(**kwargs)

    def wrap(self, value):
        if not isinstance(value, ObjectId):
            raise BadValueException()
        return value
    unwrap = wrap

class DictField(Field):
    def __init__(self, key_type, value_type, **kwargs):
        super(DictField, self).__init__(**kwargs)
        self.key_type = key_type
        self.value_type = value_type
        if not isinstance(key_type, Field):
            raise Exception("DictField key type is not a field!")
        if not isinstance(value_type, Field):
            raise Exception("DictField value type is not a field!")

    def wrap(self, value):
        ret = {}
        for k, v in value.iteritems():
            ret[self.key_type.wrap(k)] = self.value_type.wrap(v)
        return ret

    def unwrap(self, value):
        ret = {}
        for k, v in value.iteritems():
            ret[self.key_type.unwrap(k)] = self.value_type.unwrap(v)
        return ret

