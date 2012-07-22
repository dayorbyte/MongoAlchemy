from nose.tools import *
from mongoalchemy.fields import *
from test.util import known_failure

# DictField
@raises(BadValueException)
def dict_wrong_type_test_wrap():
    DictField(IntField()).wrap(4)

@raises(BadValueException)
def dict_bad_type_test_unwrap():
    DictField(IntField()).unwrap(4)

@raises(BadValueException)
def dict_wrong_value_type_test_wrap():
    DictField(StringField()).wrap({ 'a' : 4})

@raises(BadValueException)
def dict_wrong_value_type_test_unwrap():
    DictField(StringField()).unwrap({ 'a' : 4})

@raises(BadFieldSpecification)
def dict_bad_value_type_test_wrap():
    DictField(int).wrap({'a' : 4})

def dict_value_test():
    s = DictField(StringField())
    foo = {'a' : 'b'}
    assert s.unwrap(s.wrap(foo)) == foo

@raises(BadValueException)
def bad_key_integer_wrap_test():
    DictField(StringField()).wrap({1 : 'b'})


@raises(BadValueException)
def bad_key_dot_wrap_test():
    DictField(StringField()).wrap({'a.b' : 'b'})

@raises(BadValueException)
def bad_key_dollar_wrap_test():
    DictField(StringField()).wrap({'a$b' : 'b'})

@raises(BadValueException)
def bad_key_dot_unwrap_test():
    DictField(StringField()).unwrap({'a.b' : 'b'})

@raises(BadValueException)
def bad_key_dollar_unwrap_test():
    DictField(StringField()).unwrap({'a$b' : 'b'})

def dict_default_test():
    s = DictField(StringField(), default_empty=True)
    assert s.default == {}


# KVField

def kv_test_autoload():
    s = KVField(IntField(), RefField(), default_empty=True)
    assert s.has_autoload
    # assert s.default == {}

def kv_default_test():
    s = KVField(StringField(), StringField(), default_empty=True)
    assert s.default == {}
    s = KVField(StringField(), StringField(), default={'a':1})
    assert s.default == {'a':1}

@raises(BadValueException)
def kv_wrong_type_test_wrap():
    KVField(IntField(), IntField()).wrap(4)

@raises(BadValueException)
def kv_bad_type_test_unwrap():
    KVField(IntField(), IntField()).unwrap(4)



@raises(BadValueException)
def kv_wrong_value_type_test_wrap():
    KVField(StringField(), StringField()).wrap({ 'a' : 4 })

@raises(BadValueException)
def kv_wrong_key_type_test_wrap():
    KVField(StringField(), StringField()).wrap({ 4 : 'a'})

@raises(BadValueException)
def kv_wrong_value_type_test_unwrap():
    KVField(StringField(), StringField()).unwrap([{'k':'a', 'v':4}])

@raises(BadValueException)
def kv_wrong_key_type_test_unwrap():
    KVField(StringField(), StringField()).unwrap([{'k' : 4, 'v' : 'a'}])



@raises(BadFieldSpecification)
def kv_bad_key_type_test():
    KVField(int, IntField())

@raises(BadFieldSpecification)
def kv_bad_key_type_test2():
    KVField(IntField(), int)

@raises(BadValueException)
def kv_bad_key_value_none_test():
    KVField(IntField(), IntField()).unwrap([{'k' : None, 'v' : 1}])


@raises(BadFieldSpecification)
def kv_bad_value_type_test():
    KVField(IntField(), int)



def kv_value_test():
    s = KVField(StringField(), IntField())
    foo = {'a' : 4, 'v' : 9}
    assert s.unwrap(s.wrap(foo)) == foo



@raises(BadValueException)
def kv_broken_kv_obj_test():
    s = KVField(StringField(), IntField())
    s.unwrap([{'k' : 'a', 'value' : 5}])
    
@raises(BadValueException)
def kv_broken_kv_obj_test2():
    s = KVField(StringField(), IntField())
    s.unwrap([('a', 5)])

