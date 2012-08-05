from nose.tools import *
from mongoalchemy.fields import *

from mongoalchemy.exceptions import DocumentException, MissingValueException, \
        ExtraValueException, FieldNotRetrieved, BadFieldSpecification
from mongoalchemy.document import Document, document_type_registry
from mongoalchemy.session import Session
from test.util import known_failure
from datetime import datetime
from bson.dbref import DBRef
from bson import ObjectId

def get_session(cache_size=0):
    s = Session.connect('unit-testing', cache_size=cache_size)
    s.clear_collection(A)
    s.clear_collection(C)
    return s

def test_setup():
    document_type_registry.clear()

class A(Document):
    x = IntField()
class AA(Document):
    x = IntField()
class B(Document):
    y = RefField(DocumentField(A))
class C(Document):
    y = RefField(DocumentField(A))

# Field Tests

def test_proxy():
    class B(Document):
        b = IntField(default=3)
    class A(Document):
        x_ids = ListField(RefField(B, allow_none=True), iproxy='xs', default_empty=True, allow_none=True)
        x_id = RefField(B, proxy='x', allow_none=True)
    
    s = get_session()
    a = A()
    for i in range(0, 3):
        b = B(b=i)
        s.insert(b)
        a.x_id = b.to_ref()
        a.x_ids.append(b.to_ref())

    s.insert(a)
    aa = s.query(A).one()
    assert aa.x.b == 2, aa.x.b
    assert [z.b for z in aa.xs] == range(0, 3)

    a_none = A(x_id=None, x_ids=[None])
    a_none._set_session(s)
    assert a_none.x == None
    assert list(a_none.xs) == [None]

    a_set = A()
    a_set.x = b

def test_proxy_ignore_missing():
    class B(Document):
        b = IntField(default=3)
    class A(Document):
        x_ids = ListField(RefField(B), iproxy='xs', default_empty=True, 
                          ignore_missing=True)
        x_id = RefField(B, proxy='x')
    
    s = get_session()
    a = A()
    for i in range(0, 3):
        b = B(b=i)
        b.mongo_id = ObjectId()
        if i > 0:
            s.insert(b)

        a.x_id = b.to_ref()
        a.x_ids.append(b.to_ref())

    s.insert(a)
    aa = s.query(A).one()
    
    assert len(list(aa.xs)) == 2, len(list(aa.xs))

def test_dereference():
    class A(Document):
        x = IntField()
    class B(Document):
        y = RefField(DocumentField(A))

    a = A(x=5)
    s = get_session(cache_size=10)
    s.insert(a)
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbref = DBRef(collection='A', id=a.mongo_id)

    assert s.dereference(a).x == 5

def test_ref_with_cache():
    class A(Document):
        x = IntField()
    class B(Document):
        y = RefField(A, proxy='yyy')

    s = get_session(cache_size=10)
    a = A(x=5)
    s.insert(a)
    b = B(y=a.to_ref())
    s.insert(b)

    s2 = get_session(cache_size=10)
    b2 = s2.query(B).filter_by(mongo_id=b.mongo_id).one()
    assert id(s.dereference(b2.y)) == id(a)


def test_unwrap():
    class A(Document):
        x = IntField()
    s = get_session()
        
    a = A(x=5)
    s.insert(a)
    
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbaref = DBRef(db='unit-testing', collection='A', id=a.mongo_id)

    ret = RefField(DocumentField(A)).unwrap(dbaref)
    assert isinstance(ret, DBRef), ret

    ret = SRefField(A).unwrap(a.mongo_id)
    assert isinstance(ret, ObjectId), ret

@raises(BadValueException)
def test_unwrap_bad_type():
    class A(Document):
        x = IntField()
    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbaref = DBRef(db='unit-testing', collection='A', id=a.mongo_id)

    ret = RefField(DocumentField(A)).unwrap(5)

def test_simple():
    class A(Document):
        x = IntField()
    a = A(x=5)
    s = get_session()
    s.insert(a)

    id = ObjectId()
    assert SRefField(A).wrap(id) == id

@raises(BadValueException)
def test_bad_collection():
    a = RefField(A)
    a.validate_unwrap(DBRef(id=ObjectId(), collection='B'))

@raises(BadValueException)
def test_bad_db():
    a = RefField(A, db='blah')
    a.validate_unwrap(DBRef(id=ObjectId(), collection='A', database='blah2'))


@raises(BadValueException)
def test_validate():
    SRefField(A).validate_unwrap(3)


