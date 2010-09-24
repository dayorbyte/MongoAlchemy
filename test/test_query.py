from nose.tools import *
from pprint import pprint
from mongomapper.session import Session, FailedOperation
from mongomapper.document import Document, Index, DocumentField
from mongomapper.fields import *
from test.util import known_failure

class T(Document):
    i = IntField()
    j = IntField(required=False)
    l = ListField(IntField(), required=False)
    index = Index().ascending('i')

class T2(Document):
    t = DocumentField(T)

def get_session():
    return Session.connect('unit-testing')

def test_update():
    s = get_session()
    s.clear_collection(T)
    
    obj = T(i=3)
    
    s.insert(obj)

    for o in s.query(T):
        assert o.i == 3
    
    s.query(T).filter(T.f.i==3).set(T.f.i, 4).execute()
    
    for o in s.query(T):
        assert o.i == 4

@raises(Exception)
def repeated_field_query_test():
    s = get_session()
    s.clear_collection(T)
    s.query(T).filter(T.f.i==3).filter(T.f.i==3)

@raises(Exception)
def repeated_field_query_test2():
    s = get_session()
    s.clear_collection(T)
    s.query(T).filter(T.f.i==3, T.f.i==3)

def nested_field_query_test():
    s = get_session()
    s.clear_collection(T, T2)
    s.query(T2).filter(T2.f.t.i==3, T2.f.t.j==2)

@known_failure
@raises(Exception)
def repeated_field_update_test():
    s = get_session()
    s.clear_collection(T)
    s.query(T).filter(T.f.i==3).set(T.f.i, 4).set(T.f.i, 5)

@known_failure
def test_comparators():
    # once filters have more sanity checking, this test will need to be broken up
    s = get_session()
    s.clear_collection(T)
    query_obj = s.query(T).filter(T.f.i == 1, 
        T.f.i < 2, 
        T.f.i > 3,
        T.f.i != 4,
        T.f.i <= 5,
        T.f.i >= 6).query
    
    assert query_obj == None

def test_name_generation():
    s = get_session()
    s.clear_collection(T)
    assert str(T.f.i) == 'i'


#
#   Update Operation Tests
#

def update_test_setup():
    s = get_session()
    s.clear_collection(T)
    return s.query(T)


def set_test():
    q = update_test_setup()
    assert q.set(T.f.i, 5).update_data == {
        '$set' : { 'i' : 5 }
    }

def unset_test():
    q = update_test_setup()
    assert q.unset(T.f.i).update_data == {
        '$unset' : { 'i' : True }
    }
    
def inc_test():
    q = update_test_setup()
    assert q.inc(T.f.i, 1).update_data == {
        '$inc' : { 'i' : 1 }
    }
    
def append_test():
    q = update_test_setup()
    assert q.append(T.f.l, 1).update_data == {
        '$push' : { 'l' : 1 }
    }
    
def extend_test():
    q = update_test_setup()
    assert q.extend(T.f.l, *(1, 2, 3)).update_data == {
        '$pushAll' : { 'l' : (1, 2, 3) }
    }

def remove_test():
    q = update_test_setup()
    assert q.remove(T.f.l, 1).update_data == {
        '$pull' : { 'l' : 1 }
    }
    
def remove_all_test():
    q = update_test_setup()
    assert q.remove_all(T.f.l, *(1, 2, 3)).update_data == {
        '$pullAll' : { 'l' : (1, 2, 3) }
    }


def add_to_set_test():
    q = update_test_setup()
    assert q.add_to_set(T.f.l, 1).update_data == {
        '$addToSet' : { 'l' : 1 }
    }
    
def pop_test():
    q = update_test_setup()
    assert q.pop(T.f.l, 1).update_data == {
        '$pop' : { 'l' : 1 }
    }


