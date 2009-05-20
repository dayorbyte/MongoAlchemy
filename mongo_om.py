from datetime import datetime
from pymongo.objectid import ObjectId

class BadValueException(Exception):
    pass

class MissingValueException(Exception):
    pass
    
class Field(object):
    def __init__(self, required=True):
        self.required = required
    
    def wrap(self, value):
        raise NotImplemented()
    
    def unwrap(self, value):
        raise NotImplemented()
    
    def is_valid(self, value):
        return True

class PrimitiveField(Field):
    '''Primitive fields are fields where a single constructor can be used 
        for wrapping and unwrapping an object.'''
    def __init__(self, constructor, **kwds):
        super(PrimitiveField, self).__init__(**kwds)
        self.constructor = constructor
    
    def wrap(self, value):
        if not self.is_valid(value):
            name = self.__class__.__name__
            raise BadValueException('Bad value for field of type "%s": %s' % (name, repr(value)))
        return self.constructor(value)
    unwrap = wrap

class StrField(PrimitiveField):
    def __init__(self, max_length=None, min_length=None, **kwds):
        super(UnicodeField, self).__init__(constructor=str, **kwds)
    def is_valid(self, value):
        if max_length != None and len(value) > max_length:
            return False
        if min_length != None and len(value) < max_length:
            return False
        return True

class UnicodeField(PrimitiveField):
    def __init__(self, max_length=None, min_length=None, **kwds):
        super(UnicodeField, self).__init__(constructor=unicode, **kwds)
    def is_valid(self, value):
        if max_length != None and len(value) > max_length:
            return False
        if min_length != None and len(value) < max_length:
            return False
        return True

class BoolField(PrimitiveField):
    def __init__(self, **kwds):
        super(BoolField, self).__init__(constructor=bool, **kwds)

class NumberField(PrimitiveField):
    def __init__(self, constructor, min_value=None, max_value=None, **kwds):
        super(IntField, self).__init__(constructor=constructor, **kwds)
        self.min = min_value
        self.max = max_value
    def is_valid(self, value):
        if self.min and value < self.min:
            return False
        if self.max and value > self.max:
            return False
        return True
        
class IntField(NumberField):
    def __init__(self, **kwds):
        super(IntField, self).__init__(constructor=int, **kwds)

class FloatField(NumberField):
    def __init__(self, **kwds):
        super(FloatField, self).__init__(constructor=float, **kwds)

class DatetimeField(Field):
    def __init__(self, **kwds):
        super(DatetimeField, self).__init__(**kwds)
    def wrap(self, value):
        if not isinstance(value, datetime):
            raise BadValueExcepion()
        return value
    unwrap = wrap

class ListField(Field):
    def __init__(self, item_type, **kwds):
        Field.__init__(self, **kwds)
        self.item_type = item_type
        if not isinstance(item_type, Field):
            raise Exception("List item_type is not a field!")
    
    def wrap(self, value):
        return [self.item_type.wrap(v) for v in value]
        
    def unwrap(self, value):
        return [self.item_type.unwrap(v) for v in value]

class SetField(Field):
    def __init__(self, item_type, **kwds):
        Field.__init__(self, **kwds)
        self.item_type = item_type
        if not isinstance(item_type, Field):
            raise Exception("SetField item_type is not a field!")

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
    def __init__(self, fun, computed_type, **kwds):
        super(ComputedField, self).__init__(**kwds)
        self.fun = fun
        self.computed_type = computed_type
    def wrap(self, obj):
        return self.computed_type.wrap(self.fun(obj))
    def unwrap(self, obj):
        return self.fun(obj)

class ObjectIdField(Field):
    def __init__(self, **kwds):
        super(ObjectIdField, self).__init__(**kwds)
    def wrap(self, value):
        if not isinstance(value, ObjectId):
            raise BadValueExcepion()
        return value
    unwrap = wrap

class DictField(Field):
    def __init__(self, key_type, value_type **kwds):
        Field.__init__(self, **kwds)
        self.key_type = key_type
        self.value_type = value_type
        if not isinstance(key_type, Field):
            raise Exception("DictField key_type is not a field!")
        if not isinstance(value_type, Field):
            raise Exception("DictField value_type is not a field!")
    
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

class MongoObject(object):
    object_mapping = {}
    
    _id = ObjectIdField(required=False)
    
    @staticmethod
    def register_type(cls, name = None):
        if name == None:
            name = cls.__name__
        MongoObject.object_mapping[name] = cls
    
    @classmethod
    def get_id(cls, db, oid):
        if not hasattr(cls, 'collection'):
            raise Exception('get_id requires the python class to have a "collection" attribute')
        id = ObjectId(str(oid))
        obj = db[cls.collection].find_one({'_id' : id})
        if obj == None:
            return None
        return MongoObject.unwrap(obj)
    
    def __init__(self, **kwds):
        cls = self.__class__
        for name in kwds:
            if not hasattr(cls, name) or not isinstance(getattr(cls, name), Field):
                raise Exception('Unknown keyword argument: %s' % name)
            setattr(self, name, kwds[name])
        
        for name in dir(cls):
            field = getattr(cls, name)
            if isinstance(field, ComputedField):
                setattr(self, name, field.fun(self))
    
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
        res['_type'] = cls.__name__
        return res
    
    @classmethod
    def unwrap(cls, obj):
        '''Unwrap an object returned from the mongo database.'''
        cls = MongoObject.object_mapping.get(obj['_type'], cls)
        del obj['_type']
        
        params = {}
        for k, v in obj.iteritems():
            field = getattr(cls, k)
            if isinstance(field, ComputedField):
                continue
            params[str(k)] = field.unwrap(v)

        i = cls(**params)
        return i

if __name__ == '__main__':
    # random testing
    class MO(MongoObject):
        i = IntField()
        
        def fun2(obj):
            return obj.i + 1
        ii = ComputedField(fun=fun2, computed_type=IntField())
    
    
    m = MO(i=1)
    
    # m.calc_ii()
    MongoObject.register_type(MO)
    print MongoObject.unwrap(m.wrap()).wrap()
    
