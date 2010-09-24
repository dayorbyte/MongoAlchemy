from nose.tools import *
from pprint import pprint
from mongomapper.session import Session
from mongomapper.document import Document, Index, DocumentField
from mongomapper.fields import *
from test.util import known_failure
from datetime import datetime


def get_session():
    return Session.connect('unit-testing')

@raises(Exception)
def test_unimplemented_wrap():
    class BadField(Field):
        pass
    b = BadField().wrap({})

@raises(Exception)
def test_unimplemented_unwrap():
    class BadField(Field):
        pass
    b = BadField().unwrap({})

# String Tests
@raises(BadValueException)
def string_wrong_type_test():
    StringField().wrap(4)

@raises(BadValueException)
def string_too_long_test():
    StringField(max_length=4).wrap('12345')

@raises(BadValueException)
def string_too_short_test():
    StringField(min_length=4).wrap('123')

def string_value_test():
    s = StringField()
    assert s.wrap('foo') == 'foo'
    assert s.unwrap('bar') == 'bar'

# Bool Field
@raises(BadValueException)
def bool_wrong_type_test():
    BoolField().wrap(4)

def bool_value_test():
    b = BoolField()
    assert b.wrap(True) == True
    assert b.unwrap(False) == False

# Number Fields
@raises(BadValueException)
def int_wrong_type_test():
    IntField().wrap('4')

@raises(BadValueException)
def int_too_high_test():
    IntField(max_value=4).wrap(5)

@raises(BadValueException)
def int_too_low_test():
    IntField(min_value=4).wrap(3)

def int_value_test():
    s = IntField()
    assert s.wrap(1) == 1
    assert s.unwrap(1564684654) == 1564684654

@raises(BadValueException)
def float_wrong_type_test():
    FloatField().wrap(1)

# Date/time field
@raises(BadValueException)
def datetime_wrong_type_test():
    DateTimeField().wrap(4)

@raises(BadValueException)
def datetime_too_new_test():
    DateTimeField(max_value=datetime(2009, 7, 9)).wrap(datetime(2009, 7, 10))

@raises(BadValueException)
def datetime_too_old_test():
    DateTimeField(min_value=datetime(2009, 7, 9)).wrap(datetime(2009, 7, 8))

def datetime_value_test():
    s = DateTimeField()
    assert s.wrap(datetime(2009, 7, 9)) == datetime(2009, 7, 9)
    assert s.unwrap(datetime(2009, 7, 9)) == datetime(2009, 7, 9)



# Computed Field
def computed_field_test():
    def adder(obj):
        return obj.a + obj.b
    
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        a_plus_b = ComputedField(adder, IntField())

    s = get_session()
    s.clear_collection(TestDoc2)
    
    obj = TestDoc2(a=1, b=2)
    assert obj.a_plus_b == 3
    
    s.insert(obj)
    
    for td in s.query(TestDoc2):
        break
    
    assert td.a_plus_b == obj.a_plus_b

