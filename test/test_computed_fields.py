from nose.tools import *
from mongoalchemy.fields import *
from test.util import known_failure
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField

def get_session():
    return Session.connect('unit-testing')

# Computed Field
def computed_field_db_test():
    
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        @ComputedField(IntField(), deps=[a,b])
        def a_plus_b(obj):
            return obj['a'] + obj['b']

    s = get_session()
    s.clear_collection(TestDoc2)
    obj = TestDoc2(a=1, b=2)
    assert obj.a_plus_b == 3, 'Got: %s' % obj.a_plus_b
    
    s.insert(obj)
    for td in s.query(TestDoc2):
        break
    assert td.a_plus_b == obj.a_plus_b

def test_no_deps_computed_field():
    class TestDoc2(Document):
        @ComputedField(IntField())
        def c(obj):
            return 1322
    TestDoc2().c == 1322

def computed_field_value_test():
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        @ComputedField(IntField(), deps=[a,b])
        def c(obj):
            return 6
    TestDoc2.c.unwrap(6)

@raises(BadValueException)
def computed_field_unwrap_test():
    
    
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        @ComputedField(IntField(), deps=[a,b])
        def c(obj):
            return 'some-bad-value'
    TestDoc2.c.unwrap('bad-value')

@raises(BadValueException)
def computed_field_wrap_test():
    
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        @ComputedField(IntField(), deps=[a,b])
        def c(obj):
            return 'some-bad-value'

    obj = TestDoc2(a=1, b=2)
    TestDoc2.wrap(obj)

@raises(BadValueException)
def computed_field_wrap_test_wrong_type():
    
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        @ComputedField(IntField(), deps=[a,b])
        def c(obj):
            return 'some-bad-value'

    TestDoc2.c.wrap('bad-value')

