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

import itertools
from datetime import datetime
from pymongo.objectid import ObjectId

class BadValueException(Exception):
    pass

class BadFieldSpecification(Exception):
    pass

class UNSET(object): 
    pass


class Field(object):
    auto = False
    
    def __init__(self, required=True, default=UNSET, db_field=None):
        self.required = required
        self.db_field = db_field
        if default != UNSET:
            self.default = default
    
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
            self.fail_validation(value)
    
    def validate_unwrap(self, value):
        if not self.is_valid_unwrap(value):
            self.fail_validation(value)
    
    def fail_validation(self, value):
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
    
    def unwrap(self, value):
        return self.wrap(value)


class TupleField(Field):
    ''' Represents a field which is a tuple of a fixed size with specific 
        types for each element in the field '''
    
    def __init__(self, *item_types, **kwargs):
        super(TupleField, self).__init__(**kwargs)
        self.size = len(item_types)
        self.types = item_types
    
    def is_valid_wrap(self, value):
        if not hasattr(value, '__len__') or len(value) != len(self.types):
            return False
        print value
        for field, value in itertools.izip(self.types, list(value)):
            if not field.is_valid_wrap(value):
                return False
        return True
    
    def is_valid_unwrap(self, value):
        if not hasattr(value, '__len__') or len(value) != len(self.types):
            return False
        
        for field, value in itertools.izip(self.types, value):
            if not field.is_valid_unwrap(value):
                return False
        return True
    
    def wrap(self, value):
        self.validate_wrap(value)
        ret = []
        for field, value in itertools.izip(self.types, value):
            ret.append(field.wrap(value))
        return ret
    
    def unwrap(self, value):
        self.validate_unwrap(value)
        ret = []
        for field, value in itertools.izip(self.types, value):
            ret.append(field.unwrap(value))
        return tuple(ret)

class EnumField(Field):
    ''' Represents a single value out of a list of possible values, all 
        of the same type. == is used for comparison'''
    
    def __init__(self, item_type, *values, **kwargs):
        super(EnumField, self).__init__(**kwargs)
        self.item_type = item_type
        self.values = values
        for value in values:
            self.item_type.validate_wrap(value)
    
    def is_valid_wrap(self, value):
        if not self.item_type.is_valid_wrap(value):
            return False
        for val in self.values:
            if val == value:
                return True
        return False
    
    def is_valid_unwrap(self, value):
        # we can't compare the DB value to the list since that would require 
        # actually unwrapping it.  We'll do the check in unwrap instead
        return self.item_type.is_valid_wrap(value)
    
    def wrap(self, value):
        self.validate_wrap(value)
        return self.item_type.wrap(value)
    
    def unwrap(self, value):
        self.validate_unwrap(value)
        value = self.item_type.unwrap(value)
        for val in self.values:
            if val == value:
                return val
        self.fail_validation(value)
    

class SequenceField(Field):
    def __init__(self, item_type, min_capacity=None, max_capacity=None, 
            **kwargs):
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
            k = self.key_type.wrap(k)
            v = self.value_type.wrap(v)
            ret.append( { 'k' : k, 'v' : v })
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
        takes three parameters:
        
        fun - the function to compute the value of the computed field
        computed_type - the type to use when wrapping the computed field
        deps - the names of fields on the current object which should be 
            passed in to compute the value
        
        the unwrap function takes a dictionary of K/V pairs of the 
        dependencies.  Since dependencies are declared in the class 
        definition all of the dependencies for a computed field should be
        in the class definition before the computed field itself.
    '''
    auto = True
    def __init__(self, computed_type, deps=None, **kwargs):
        super(ComputedField, self).__init__(**kwargs)
        self.computed_type = computed_type
        if deps == None:
            deps = set()
        self.deps = set(deps)
    
    def is_valid_wrap(self, value):
        return self.computed_type.is_valid_wrap(value)
    
    def is_valid_unwrap(self, value):
        return self.computed_type.is_valid_unwrap(value)
    
    def wrap(self, value):
        self.validate_wrap(value)
        return self.computed_type.wrap(value)
    
    def unwrap(self, value):
        self.validate_unwrap(value)
        return self.computed_type.unwrap(value)
    
    def __call__(self, fun):
        return ComputedFieldValue(self, fun)

class ComputedFieldValue(property, ComputedField):
    class UNSET(object): pass
    
    def __init__(self, field, fun):
        self.__computed_value = self.UNSET
        self.field = field
        self.fun = fun
    
    def compute_value(self, doc):
        args = {}
        for dep in self.field.deps:
            args[dep.name] = getattr(doc, dep.name)
        value = self.fun(args)
        if not self.field.computed_type.is_valid_wrap(value):
            raise BadValueException('Computed Function return a bad value')
        return value
    
    def __set__(self, instance, value):
        if self.field.is_valid_wrap(value):
            self.__computed_value = value
            return
        # TODO: this line should be impossible to reach, but I'd like an 
        # exception just in case, but then I can't have full coverage!
        # raise BadValueException('Tried to set a computed field to an illegal value: %s' % value)
    
    def __get__(self, instance, owner):
        if instance == None:
            return self.field
        # TODO: dirty cache indictor + check a field option for never caching
        if self.__computed_value == self.UNSET:
            self.__computed_value = self.compute_value(instance)
        return self.__computed_value
