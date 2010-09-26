from nose.tools import *
from mongoalchemy.fields import *
from test.util import known_failure
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField

def get_session():
    return Session.connect('unit-testing')

# Computed Field
def computed_field_db_test():
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

def computed_field_value_test():
    def adder(obj):
        return 1
    
    c = ComputedField(adder, IntField())
    assert c.wrap(c.unwrap(None)) == 1

@raises(BadValueException)
def computed_field_unwrap_test():
    def adder(obj):
        return 'some-bad-value'
    
    c = ComputedField(adder, IntField())
    c.unwrap(None)

@raises(BadValueException)
def computed_field_wrap_test():
    def adder(obj):
        return 'some-bad-value'
    
    c = ComputedField(adder, IntField())
    c.wrap(None)