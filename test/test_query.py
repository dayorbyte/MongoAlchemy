from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField, FieldNotRetrieved
from mongoalchemy.fields import *
from mongoalchemy.query import BadQueryException, Query, BadResultException
from test.util import known_failure

class T(Document):
    i = IntField()
    j = IntField(required=False)
    l = ListField(IntField(), required=False)
    a = IntField(required=False, db_field='aa')
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

def test_field_filter():
    s = get_session()
    s.clear_collection(T, T2)
    
    # Simple Object
    obj = T(i=3)
    s.insert(obj)
    for t in s.query(T).fields(T.f.i):
        break
    assert t.i == 3
    # Nested Object
    obj2 = T2(t=obj)
    s.insert(obj2)
    for t2 in s.query(T2).fields(T2.f.t.i):
        break
    assert t2.t.i == 3

def test_limit():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    s.insert(T(i=5))
    for count, item in enumerate(s.query(T).limit(2)):
        pass
    assert count == 1

def test_skip():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    s.insert(T(i=5))
    for count, item in enumerate(s.query(T).skip(2)):
        pass
    assert count == 0

def test_hint():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    for item in s.query(T).hint_asc(T.f.i):
        pass

@raises(BadQueryException)
def test_hint_validation():
    s = get_session()
    s.query(T).hint_asc(T.f.i).hint_desc(T.f.i)

@raises(BadResultException) # too many values to unpack
def test_one_fail():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    s.query(T).one()

@raises(BadResultException) # too few values to unpack
def test_one_fail_too_few():
    s = get_session()
    s.clear_collection(T)
    s.query(T).one()


def test_one():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    assert s.query(T).one().i == 3

def test_first():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    assert s.query(T).descending(T.f.i).first().i == 4
    assert s.query(T).ascending(T.f.i).first().i == 3

def test_first_empty():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    assert s.query(T).filter(T.f.i > 5).first() == None

def test_all():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    for count, item in enumerate(s.query(T).all()):
        pass
    assert count == 1

def test_distinct():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3, j=4))
    s.insert(T(i=3, j=5))
    s.insert(T(i=3, j=6))
    for count, item in enumerate(s.query(T).distinct(T.f.i)):
        pass
    assert count == 0, count

def test_count():
    s = get_session()
    s.clear_collection(T)
    assert s.query(T).count() == 0
    s.insert(T(i=3, j=4))
    s.insert(T(i=3, j=5))
    s.insert(T(i=3, j=6))
    assert s.query(T).count() == 3

def test_explain():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    assert 'allPlans' in s.query(T).filter(T.f.i > 5).explain()


def test_clone():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    q = s.query(T)
    q2 = q.clone()
    q.skip(2)
    for count, item in enumerate(q2):
        pass
    assert count == 1

@raises(FieldNotRetrieved)
def test_field_filter_non_retrieved_field():
    s = get_session()
    s.clear_collection(T)
    obj = T(i=3, j=2)
    s.insert(obj)
    for t in s.query(T).fields(T.f.i):
        break
    assert t.j == 2

@raises(FieldNotRetrieved)
def test_field_filter_non_retrieved_subdocument_field():
    s = get_session()
    s.clear_collection(T, T2)
    obj = T(i=3, j=2)
    obj2 = T2(t=obj)
    s.insert(obj2)
    for t2 in s.query(T2).fields(T2.f.t.i):
        break
    assert t2.t.j == 2

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

def test_comparators():
    # once filters have more sanity checking, this test will need to be broken up
    s = get_session()
    query_obj = s.query(T).filter(T.f.i < 2, 
        T.f.i > 3,
        T.f.i != 4,
        T.f.i <= 5,
        T.f.i >= 6).query
    
    assert query_obj == {'i': {'$ne': 4, '$gte': 6, '$lte': 5, '$gt': 3, '$lt': 2}}
    
    s.query(T).filter(T.f.i == 1).query == { 'i' : 1}

@raises(BadQueryException)
def invalid_combination_test():
    s = get_session()
    s.query(T).filter(T.f.i < 2, T.f.i == 4)

def test_sort():
    from pymongo import ASCENDING, DESCENDING
    s = get_session()
    sorted_query = s.query(T).ascending(T.f.i).descending(T.f.j)
    assert sorted_query.sort == [('i', ASCENDING),('j', DESCENDING)], sorted_query.sort
    for obj in sorted_query:
        pass

@raises(BadQueryException)
def test_sort_by_same_key():
    s = get_session()
    sorted_query = s.query(T).ascending(T.f.i).descending(T.f.i)


#
#   QueryResult tests
#

def qr_test_misc():
    s = get_session()
    cursor = iter(s.query(T))
    assert cursor.__iter__() == cursor

def qr_test_getitem():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    assert s.query(T).descending(T.f.i)[0].i == 4

def qr_test_rewind():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    it = iter(s.query(T))
    it.next()
    it.next()
    it.rewind()
    it.next()
    it.next()
    try:
        it.next()
    except StopIteration:
        pass
    
def qr_test_clone():
    s = get_session()
    s.clear_collection(T)
    s.insert(T(i=3))
    s.insert(T(i=4))
    it = iter(s.query(T))
    it.next()
    it.next()
    it2 = it.clone()
    it2.next()
    it2.next()
    try:
        it2.next()
    except StopIteration:
        pass
    