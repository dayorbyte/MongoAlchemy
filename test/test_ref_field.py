from nose.tools import *
from mongoalchemy.fields import *

from mongoalchemy.exceptions import DocumentException, MissingValueException, \
        ExtraValueException, FieldNotRetrieved, BadFieldSpecification, \
        BadReferenceException
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

def test_simple_dereference():
    print 1111
    class ASD(Document):
        x = IntField()
    class BSD(Document):
        y_id = SRefField(DocumentField(ASD))
        y = y_id.rel()

    s = get_session()
    s.clear_collection(ASD)
    s.clear_collection(BSD)
    a = ASD(x=4)
    s.insert(a)

    b = BSD()
    b.y = a
    s.add_to_session(b)
    assert b.y.x == 4

def test_poly_ref():
    class PRef(Document):
        config_polymorphic_collection = True
        x = IntField()
    
    class PRef2(PRef):
        y = IntField()
    r2 = PRef2()
    r2.mongo_id = ObjectId()
    assert RefField()._to_ref(r2).collection == 'PRef'


def test_proxy():
    class TPB(Document):
        b = IntField(default=3)
    class TPA(Document):
        x_ids = ListField(RefField(TPB, allow_none=True), default_empty=True, allow_none=True)
        xs = x_ids.rel()
        x_id = RefField(TPB, allow_none=True)
        x = x_id.rel()
    
    s = get_session()
    s.clear_collection(TPA)
    s.clear_collection(TPB)

    a = TPA()
    for i in range(0, 3):
        b = TPB(b=i)
        s.insert(b)
        a.x_id = b.to_ref()
        a.x_ids.append(b.to_ref())

    s.insert(a)
    aa = s.query(TPA).one()
    assert aa.x.b == 2, aa.x.b
    assert [z.b for z in aa.xs] == range(0, 3)

    a_none = TPA(x_id=None, x_ids=[None])
    a_none._set_session(s)
    assert a_none.x == None
    assert list(a_none.xs) == [None]

    a_set = TPA()
    a_set.x = b

def test_proxy_ignore_missing():
    class TPIMB(Document):
        b = IntField(default=3)
    class TPIMA(Document):
        x_ids = ListField(RefField(TPIMB), default_empty=True)
        xs = x_ids.rel(ignore_missing=True)
        x_id = RefField(TPIMB)
        x = x_id.rel()
    
    s = get_session()
    s.clear_collection(TPIMA)
    s.clear_collection(TPIMB)

    a = TPIMA()
    for i in range(0, 3):
        b = TPIMB(b=i)
        b.mongo_id = ObjectId()
        if i > 0:
            s.insert(b)

        a.x_id = b.to_ref()
        a.x_ids.append(b.to_ref())

    s.insert(a)
    aa = s.query(TPIMA).one()
    
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

@raises(BadValueException)
def test_unwrap_missing_db():
    class A(Document):
        x = IntField()
    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbaref = DBRef(collection='A', id=a.mongo_id)

    ret = RefField(DocumentField(A), db_required=True).unwrap(dbaref)

def test_dereference_doc():
    class A(Document):
        x = IntField()
    
    s = Session.connect('unit-testing', cache_size=0)
    s.clear_collection(A)

    a = A(x=5)
    s.insert(a)
    dbaref = DBRef(collection='A', id=a.mongo_id, database='unit-testing')
    s2 = Session.connect('unit-testing2', cache_size=0)
    assert s2.dereference(a).x == 5

def test_dereference():
    class A(Document):
        x = IntField()
    
    s = Session.connect('unit-testing', cache_size=0)
    s.clear_collection(A)

    a = A(x=5)
    s.insert(a)
    dbaref = DBRef(collection='A', id=a.mongo_id, database='unit-testing')
    s2 = Session.connect('unit-testing2', cache_size=0)
    assert s2.dereference(dbaref).x == 5

@raises(BadReferenceException)
def test_bad_dereference():
    class A(Document):
        x = IntField()
    
    s = Session.connect('unit-testing', cache_size=0)
    s.clear_collection(A)
    dbaref = DBRef(collection='A', id=ObjectId(), database='unit-testing')
    s.dereference(dbaref)


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


