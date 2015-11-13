from __future__ import print_function
from mongoalchemy.py3compat import *

from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, FieldNotRetrieved
from mongoalchemy.fields import *
from mongoalchemy.query import BadQueryException, Query, BadResultException
from test.util import known_failure
import pymongo

PYMONGO_3 = pymongo.version_tuple >= (3, 0, 0)

class T(Document):
    i = IntField()
    j = IntField(required=False)
    l = ListField(IntField(), required=False)
    a = IntField(required=False, db_field='aa')
    index = Index().ascending('i')

class T2(Document):
    t = DocumentField(T)

class T3(Document):
    t_list = ListField(DocumentField(T))

def get_session():
    s = Session.connect('unit-testing')
    s.clear_collection(T, T2, T3)
    return s

def test_elem_match_field():
    s = get_session()
    match = T3.t_list.elem_match({'i':1})
    q = s.query(T3).fields(match)
    expr = q._fields_expression()
    expected = {
        't_list' : {
            '$elemMatch' : {'i': 1}
        },
        '_id' : True,
    }
    assert expr == expected, q._fields_expression()
    assert str(match) == 't_list'

def test_fields_exclude():
    s = get_session()
    q = s.query(T3).fields(T3.mongo_id.exclude())
    expr = q._fields_expression()
    expected = {
        '_id' : False,
    }
    assert expr == expected, q._fields_expression()

def test_update():
    s = get_session()

    obj = T(i=3)

    s.save(obj)

    for o in s.query(T):
        assert o.i == 3

    s.query(T).filter(T.i==3).set(T.i, 4).execute()

    for o in s.query(T):
        assert o.i == 4

def test_field_filter():
    s = get_session()
    s.clear_collection(T, T2)

    # Simple Object
    obj = T(i=3)
    s.save(obj)
    for t in s.query(T).fields(T.i):
        break
    assert t.i == 3
    # Nested Object
    obj2 = T2(t=obj)
    s.save(obj2)
    for t2 in s.query(T2).fields(T2.t.i):
        break
    assert t2.t.i == 3

def test_raw_output():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    value = s.query(T).raw_output().one()
    assert isinstance(value, dict)

def test_limit():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    s.save(T(i=5))
    for count, item in enumerate(s.query(T).limit(2)):
        pass
    assert count == 1

def test_skip():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    s.save(T(i=5))
    for count, item in enumerate(s.query(T).skip(2)):
        pass
    assert count == 0

def test_hint():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    for item in s.query(T).hint_asc(T.i):
        pass

@raises(BadQueryException)
def test_hint_validation():
    s = get_session()
    s.query(T).hint_asc(T.i).hint_desc(T.i)

@raises(BadResultException) # too many values to unpack
def test_one_fail():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    s.query(T).one()

@raises(BadResultException) # too few values to unpack
def test_one_fail_too_few():
    s = get_session()
    s.clear_collection(T)
    s.query(T).one()


def test_one():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    assert s.query(T).one().i == 3

def test_first():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    assert s.query(T).descending(T.i).first().i == 4
    assert s.query(T).ascending(T.i).first().i == 3

def test_first_empty():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    assert s.query(T).filter(T.i > 5).first() is None

def test_all():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    for count, item in enumerate(s.query(T).all()):
        pass
    assert count == 1

def test_distinct():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3, j=4))
    s.save(T(i=3, j=5))
    s.save(T(i=3, j=6))
    for count, item in enumerate(s.query(T).distinct(T.i)):
        pass
    assert count == 0, count

def test_count():
    s = get_session()
    s.clear_collection(T)
    assert s.query(T).count() == 0
    s.save(T(i=3, j=4))
    s.save(T(i=3, j=5))
    s.save(T(i=3, j=6))
    assert s.query(T).count() == 3

def test_explain():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    explain = s.query(T).filter(T.i > 5).explain()
    assert 'executionStats' in explain or 'allPlans' in explain, explain


def test_clone():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    q = s.query(T)
    q2 = q.clone()
    q.skip(2)
    for count, item in enumerate(q2):
        pass
    assert count == 1

def test_raw_query():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    assert s.query(T).filter({'i':3}).one().i == 3

@raises(FieldNotRetrieved)
def test_field_filter_non_retrieved_field():
    s = get_session()
    s.clear_collection(T)
    obj = T(i=3, j=2)
    s.save(obj)
    for t in s.query(T).fields(T.i):
        break
    assert t.j == 2

@raises(FieldNotRetrieved)
def test_field_filter_non_retrieved_subdocument_field():
    s = get_session()
    s.clear_collection(T, T2)
    obj = T(i=3, j=2)
    obj2 = T2(t=obj)
    s.save(obj2)
    for t2 in s.query(T2).fields(T2.t.i):
        break
    assert t2.t.j == 2

@raises(FieldNotRetrieved)
def test_save_partial_subdocument_fail():
    class Foo(Document):
        a = DocumentField('Bar')
    class Bar(Document):
        b = IntField()
        c = IntField()
    s = get_session()
    s.clear_collection(Foo)
    s.clear_collection(Bar)
    bar = Bar(b=1432, c=1112)
    s.save(bar)
    bar = s.query(Bar).filter_by(b=1432, c=1112).fields('c').one()
    s.save(Foo(a=bar))
    s.query(Foo).filter(Foo.a.c==1112).one()


def test_save_partial_subdocument():
    class Foo(Document):
        a = DocumentField('Bar')
    class Bar(Document):
        b = IntField(required=False)
        c = IntField()
    s = get_session()
    s.clear_collection(Foo)
    s.clear_collection(Bar)
    bar = Bar(b=1432, c=1112)
    s.save(bar)
    bar = s.query(Bar).filter_by(b=1432, c=1112).fields('c').one()
    s.save(Foo(a=bar))
    s.query(Foo).filter(Foo.a.c==1112).one()


@raises(Exception)
def repeated_field_query_test():
    s = get_session()
    s.clear_collection(T)
    s.query(T).filter(T.i==3).filter(T.i==3)

@raises(Exception)
def repeated_field_query_test2():
    s = get_session()
    s.clear_collection(T)
    s.query(T).filter(T.i==3, T.i==3)

def nested_field_query_test():
    s = get_session()
    s.clear_collection(T, T2)
    assert s.query(T2).filter(T2.t.i==3, T2.t.j==2).query == {'t.i':3, 't.j':2}

@known_failure
@raises(Exception)
def repeated_field_update_test():
    s = get_session()
    s.clear_collection(T)
    s.query(T).filter(T.i==3).set(T.i, 4).set(T.i, 5)

def test_comparators():
    # once filters have more sanity checking, this test will need to be broken up
    s = get_session()
    query_obj = s.query(T).filter(T.i < 2,
        T.i > 3,
        T.i != 4,
        T.i <= 5,
        T.i >= 6).query

    assert query_obj == {'i': {'$ne': 4, '$gte': 6, '$lte': 5, '$gt': 3, '$lt': 2}}

    s.query(T).filter(T.i == 1).query == { 'i' : 1}


@raises(BadQueryException)
def invalid_combination_test():
    s = get_session()
    s.query(T).filter(T.i < 2, T.i == 4)


def test_sort():
    from pymongo import ASCENDING, DESCENDING
    s = get_session()
    sorted_query = s.query(T).ascending(T.i).descending(T.j)
    assert sorted_query._sort == [('i', ASCENDING),('j', DESCENDING)], sorted_query._sort
    for obj in sorted_query:
        pass

def test_sort2():
    from pymongo import ASCENDING, DESCENDING
    s = get_session()
    sorted_query = s.query(T).sort((T.i, ASCENDING), ('j', DESCENDING))
    assert sorted_query._sort == [('i', ASCENDING),('j', DESCENDING)], sorted_query._sort

@raises(BadQueryException)
def test_sort_bad_dir():
    from pymongo import ASCENDING, DESCENDING
    s = get_session()
    sorted_query = s.query(T).sort((T.i, ASCENDING), ('j', 4))
    assert sorted_query._sort == [('i', ASCENDING),('j', DESCENDING)], sorted_query._sort

@raises(BadQueryException)
def test_sort_by_same_key():
    s = get_session()
    sorted_query = s.query(T).ascending(T.i).descending(T.i)


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
    s.save(T(i=3))
    s.save(T(i=4))
    assert s.query(T).descending(T.i)[0].i == 4

def qr_test_rewind():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    it = iter(s.query(T))
    next(it)
    next(it)
    it.rewind()
    next(it)
    next(it)
    try:
        next(it)
    except StopIteration:
        pass

def qr_test_clone():
    s = get_session()
    s.clear_collection(T)
    s.save(T(i=3))
    s.save(T(i=4))
    it = iter(s.query(T))
    next(it)
    next(it)
    it2 = it.clone()
    next(it2)
    next(it2)
    try:
        next(it2)
    except StopIteration:
        pass

def test_resolve_fields():
    class Resolver(Document):
        i = IntField(db_field='j')
        k = IntField()
    s = get_session()
    s.clear_collection(Resolver)

    q = s.query(Resolver).filter(Resolver.i.in_(6))
    q = s.query(Resolver).set(Resolver.i, 6)

