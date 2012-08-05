from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, FieldNotRetrieved
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

class NestedChild(Document):
    i = IntField()
class NestedParent(Document):
    l = ListField(DocumentField(NestedChild))


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

def test_nested_matching():
    assert str(NestedParent.l.i) == 'l.i'

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
# Value Encoding Type tests
#
def test_value_type_wrapping():
    class User(Document):
        bio = SetField(StringField())
    s = get_session()
    s.clear_collection(User)
    
    q = s.query(User).in_(User.bio, 'MongoAlchemy').query
    assert q == { 'bio' : { '$in' : ['MongoAlchemy'] } }, q
    
    q = s.query(User).in_(User.bio, set(['MongoAlchemy'])).query
    assert q == { 'bio' : { '$in' : [['MongoAlchemy']] } }, q

def test_value_type_wrapping_2():
    class User(Document):
        bio = KVField(StringField(), IntField())
    s = get_session()
    s.clear_collection(User)
    q = s.query(User).in_(User.bio.k, 'MongoAlchemy').query
    assert q == { 'bio.k' : { '$in' : ['MongoAlchemy'] } }, q
    
    q = s.query(User).in_(User.bio, { 'MongoAlchemy' : 5}).query    
    assert q == { 'bio' : { '$in' : [[{'k': 'MongoAlchemy', 'v': 5}]] } }, q

@raises(BadValueException)
def test_value_type_wrapping_wrong_twice():
    class User(Document):
        bio = SetField(StringField())
    s = get_session()
    s.query(User).in_(User.bio, 1).query == { 'bio' : { '$in' : ['MongoAlchemy'] } }

def list_in_operator_test():
    class User(Document):
        ints = ListField(IntField())
    s = get_session()
    s.clear_collection(User)
    
    q = s.query(User).filter_by(ints=3).query
    assert q == { 'ints' : 3 }, q
    
    q = s.query(User).filter(User.ints == 3).query
    assert q == { 'ints' : 3 }, q
    
    q = s.query(User).filter(User.ints == [3]).query
    assert q == { 'ints' : [3] }, q

#
#   Geo Tests
#
def test_geo():
    class Place(Document):
        config_collection_name = 'places4'
        loc = GeoField()
        val = IntField()
        index = Index().geo2d('loc', min=-100, max=100)
    s = Session.connect('unit-testing')
    s.clear_collection(Place)
    s.insert(Place(loc=(1,1), val=2))
    s.insert(Place(loc=(5,5), val=4))
    s.insert(Place(loc=(30,30 ), val=5))
    x = s.query(Place).filter(Place.loc.near(0, 1))
    assert x.first().val == 2, x.query

    xs = s.query(Place).filter(Place.loc.near(1, 1, max_distance=2)).all()
    assert len(xs) == 1, xs
    
    xs = s.query(Place).filter(Place.loc.near_sphere(1, 1, max_distance=50)).all()
    assert len(xs) == 3

    q = s.query(Place).filter(Place.loc.within_box([-2, -2], [2, 2]))
    assert len(q.all()) == 1, q.query

    q = s.query(Place).filter(Place.loc.within_radius(0, 0, 2))
    assert len(q.all()) == 1, q.query

    q = s.query(Place).filter(Place.loc.within_polygon(
        [[-2, 0], [2, 0], [0, 2], [0, -2]]
    ))
    assert len(q.all()) == 1, q.query

    q = s.query(Place).filter(Place.loc.within_radius_sphere(30, 30, 0.0001))
    assert len(q.all()) == 1, q.all()


def test_geo_haystack():
    class Place(Document):
        config_collection_name = 'places'
        loc = GeoField()
        val = IntField()
        index = Index().geo_haystack('loc', bucket_size=100).descending('val')
    s = Session.connect('unit-testing')
    s.clear_collection(Place)
    s.insert(Place(loc=(1,1), val=2))
    s.insert(Place(loc=(5,5), val=4))
    



#
#  Comparator Tests
#
@raises(BadValueException)
def qf_bad_value_equals_test():
    T2.t.i == '3'

@raises(BadValueException)
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

def test_exists():
    q = Query(T, None)
    assert q.filter(T.i.exists(False)).query == {'i': {'$exists': False}}
    assert q.filter(T.i.exists(True)).query == {'i': {'$exists': True}}


# free-form queries

def test_ffq():
    s = get_session()
    q = s.query('T')
    assert q.filter(Q.name == 3).query == {'name' : 3}
    
    q = s.query('T').filter(Q.name.first == 'jeff').query
    assert q == {'name.first' : 'jeff'}, q

    s.insert(T(i=4))
    assert s.query('T').count() == 1
    
    assert s.query('T').filter(Q.i == 4).one()['i'] == 4

# Array Index Operator
def test_array_index_operator():
    
    assert str(NestedParent.l.matched_index().i) == 'l.$.i', NestedParent.l.matched_index().i


