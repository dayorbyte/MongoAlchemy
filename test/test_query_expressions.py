from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField, FieldNotRetrieved
from mongoalchemy.fields import *
from mongoalchemy.query import BadQueryException, Query, BadResultException
from mongoalchemy.query_expression import Q
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
    s = Session.connect('unit-testing')
    s.clear_collection(T, T2)
    return s
#
#   Test Query Fields
#

@raises(BadQueryException)
def test_sort_by_same_key():
    s = get_session()
    sorted_query = s.query(T).ascending(T.i).descending(T.i)

def test_name_generation():
    s = get_session()
    assert str(T.i) == 'i'

def test_ne():
    assert (T.i != T.j) == True
    assert (T.i != T.i) == False

def query_field_repr_test():
    assert repr(T.i) == 'QueryField(i)'

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

def test_not():
    not_q = Query(T, None).filter( ~(T.i == 3) ).query
    assert not_q == { 'i' : {'$ne' : 3} }, not_q
    
    not_q = Query(T, None).not_(T.i > 4).query
    assert not_q == { 'i' : {'$not': { '$gt': 4}} }, not_q

@raises(BadQueryException)
def test_not_with_malformed_field():
    class Any(DocumentField):
        i = AnythingField()
    not_q = Query(Any, None).not_(Any.i == { '$gt' : 4, 'garbage' : 5})

def test_not_assign_dict_malformed_field():
    class Any(Document):
        i = AnythingField()
    not_q = Query(Any, None).not_(Any.i == { 'a' : 4, 'b' : 5}).query
    assert not_q == { 'i' : { '$ne' : { 'a' : 4, 'b' : 5 } } }, not_q

def test_not_db_test():
    s = get_session()
    s.insert(T(i=5))
    assert s.query(T).not_(T.i == 5).first() == None
    assert s.query(T).not_(T.i > 6).one().i == 5

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


# free-form queries

def test_ffq():
    s = get_session()
    q = s.query('T')
    print type(Q.name), type(Q.name.first)
    assert q.filter(Q.name == 3).query == {'name' : 3}
    
    q = s.query('T').filter(Q.name.first == 'jeff').query
    assert q == {'name.first' : 'jeff'}, q

    s.insert(T(i=4))
    assert s.query('T').count() == 1
    
    assert s.query('T').filter(Q.i == 4).one()['i'] == 4