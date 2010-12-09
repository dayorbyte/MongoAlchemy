from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField, FieldNotRetrieved
from mongoalchemy.fields import *
from mongoalchemy.query import BadQueryException, Query, BadResultException
from test.util import known_failure


# TODO: Test al operators to make sure wrap is called on their values

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

#
#   Test Query Fields
#

@raises(BadQueryException)
def test_sort_by_same_key():
    s = get_session()
    sorted_query = s.query(T).ascending(T.i).descending(T.i)

def test_name_generation():
    s = get_session()
    s.clear_collection(T)
    assert str(T.i) == 'i'

#
# QueryField Tests
#
@raises(AttributeError)
def test_bad_query_field_name():
    T.q

@raises(AttributeError)
def test_subitem_of_no_subitem():
    T.i.i

def qf_parent_test():
    assert str(T2.t.i._get_parent()) == 't'

@raises(BadQueryException)
def qf_bad_subfield_test():
    assert str(T2.t.q) == 't.q'

def qf_db_name_test():
    assert str(T.a) == 'aa', str(T.a)
    
#
#  Comparator Tests
#
@raises(BadQueryException)
def qf_bad_value_equals_test():
    T2.t.i == '3'

@raises(BadQueryException)
def qf_bad_value_compare_test():
    T2.t.i < '3'

def qf_dot_f_test():
    
    class T3(Document):
        i = IntField()
        j = IntField(required=False)
        l = ListField(IntField(), required=False)
        a = IntField(required=False, db_field='aa')
        index = Index().ascending('i')

    class T4(Document):
        t = DocumentField(T3)
    
    assert str(T4.t.i) == 't.i'

# def test_not():
#     q = Query(T, None)
#     
#     assert q.filter( ~(T.i == 3) ).query == { '$not' : {'i' : 3} }
#     assert q.not_(T.i == 3).query == { '$not' : {'i' : 3} }

def test_or():
    q = Query(T, None)
    
    want = { '$or' : [{'i' : 3}, {'i' : 4}, {'i' : 5}] }
    assert q.filter((T.i == 3) | (T.i == 4) | (T.i == 5)).query == want
    
    assert Query(T, None).or_(T.i == 3, T.i == 4, T.i == 5).query == want

def test_in():
    q = Query(T, None)
    assert q.in_(T.i, 1, 2, 3).query == {'i' : {'$in' : [1,2,3]}}, q.in_(T.i, 1, 2, 3).query
    assert q.filter(T.i.in_(1, 2, 3)).query == {'i' : {'$in' : [1,2,3]}}

def test_nin():
    q = Query(T, None)
    assert q.nin(T.i, 1, 2, 3).query == {'i' : {'$nin' : [1,2,3]}}, q.nin(T.i, 1, 2, 3).query
    assert q.filter(T.i.nin(1, 2, 3)).query == {'i' : {'$nin' : [1,2,3]}}

