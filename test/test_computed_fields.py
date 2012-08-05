from nose.tools import *
from mongoalchemy.fields import *
from test.util import known_failure
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index
from mongoalchemy.query import Query

def get_session():
    return Session.connect('unit-testing')

# Computed Field
def computed_field_db_test():
    
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        @computed_field(IntField(), deps=[a,b])
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

def test_created_modified():
    class TestDoc2(Document):
        created = CreatedField()
        modified = ModifiedField()
    d = TestDoc2()
    w1 = d.wrap()
    import time
    time.sleep(0.01)
    w2 = d.unwrap(w1).wrap()
    assert w1['created'] == w2['created']
    assert w1['modified'] != w2['modified']

def test_no_deps_computed_field():
    class TestDoc2(Document):
        @computed_field(IntField())
        def c(obj):
            return 1322
    TestDoc2().c == 1322

def computed_field_value_test():
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        @computed_field(IntField(), deps=[a,b])
        def c(obj):
            return 6
    TestDoc2.c.unwrap(6)

def computed_value_update_test():
    # No deps
    class UpDoc(Document):
        @computed_field(IntField())
        def c(obj):
            return 3
    assert UpDoc().get_dirty_ops() == { '$set' : { 'c' : 3 } }, UpDoc().get_dirty_ops()
    
    # Deps, updated
    class UpDoc2(Document):
        i = IntField(required=False)
        @computed_field(IntField(), deps=[i])
        def d(obj):
            return obj['i']+1
    ud2 = UpDoc2(i=3)
    assert ud2.get_dirty_ops() == { '$set' : { 'd' : 4, 'i' : 3 } }, ud2.get_dirty_ops()

    ud3 = UpDoc2()
    assert ud3.get_dirty_ops() == {}, ud3.get_dirty_ops()

@raises(BadValueException)
def computed_field_unwrap_test():
    
    
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        @computed_field(IntField(), deps=[a,b])
        def c(obj):
            return 'some-bad-value'
    TestDoc2.c.unwrap('bad-value')

def computed_field_wrap_value_func_test():
    class TestDoc2(Document):
        @computed_field(StringField())
        def c(obj):
            return 'foo'
    Query(TestDoc2, None).in_(TestDoc2.c, 'bar').query == {'c' : { '$in' : 'bar' }}


@raises(BadValueException)
def computed_field_wrap_test():
    
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        @computed_field(IntField(), deps=[a,b])
        def c(obj):
            return 'some-bad-value'

    obj = TestDoc2(a=1, b=2)
    TestDoc2.wrap(obj)

def computed_field_wrap_value_test():
    
    class TestDoc2(Document):
        @computed_field(IntField())
        def c(obj):
            return 4

    obj = TestDoc2()
    wrapped = TestDoc2.wrap(obj)
    assert wrapped == { 'c' : 4 }, wrapped


@raises(BadValueException)
def computed_field_wrap_test_wrong_type():
    
    class TestDoc2(Document):
        a = IntField()
        b = IntField()
        @computed_field(IntField(), deps=[a,b])
        def c(obj):
            return 'some-bad-value'

    TestDoc2.c.wrap('bad-value')

# Caching Values and one-time fields

def computed_field_cached_value_test():
    from datetime import datetime, timedelta
    class CDoc(Document):
        offset = 0
        @computed_field(DateTimeField(), one_time=True)
        def created(obj):
            return datetime(2010, 11, 1) + timedelta(minutes=CDoc.offset)
        
        @computed_field(DateTimeField())
        def modified(obj):
            return datetime(2010, 11, 1) + timedelta(minutes=CDoc.offset)
    
    c = CDoc()
    created_before = c.created
    modified_before = c.modified
    CDoc.offset = 10
    created_after = c.created
    modified_after = c.modified
    
    assert created_before == created_after
    assert modified_before != modified_after
    
    c.modified = datetime(1970, 10, 10)
    assert c.modified == modified_after

@raises(BadValueException)
def computed_field_one_time_assignment_test():
    class CDoc(Document):
        @computed_field(IntField(), one_time=True)
        def created(obj):
            return 1
    
    c = CDoc()
    assert c.created == 1
    c.created = 2

