from nose.tools import *
from mongoalchemy.fields import *

from mongoalchemy.exceptions import DocumentException, MissingValueException, \
        ExtraValueException, FieldNotRetrieved, BadFieldSpecification
from mongoalchemy.document import Document, DocumentField, document_type_registry
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
    y = RefField(DocumentField(A), autoload=True)

# @raises(BadFieldSpecification)
# def test_document_field():
#     RefField(type=3)

# Field Tests

def test_proxy():
    class B(Document):
        b = IntField(default=3)
    class A(Document):
        x_ids = ListField(RefField(B), iproxy='xs', default_empty=True)
        x_id = RefField(B, proxy='x')
    
    s = get_session()
    a = A()
    for i in range(0, 3):
        b = B(b=i)
        s.insert(b)
        a.x_id = b
        a.x_ids.append(b)

    s.insert(a)
    aa = s.query(A).one()
    assert aa.x.b == 2, aa.x.b
    assert [z.b for z in aa.xs] == range(0, 3)

    a_none = A(x_id=None, x_ids=[None])
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

        a.x_id = b
        a.x_ids.append(b)

    s.insert(a)
    aa = s.query(A).one()
    
    assert len(list(aa.xs)) == 2, len(list(aa.xs))


def test_reffield():
    class A(Document):
        x = IntField()
    class AA(Document):
        x = IntField()
    class B(Document):
        y = RefField(DocumentField(A))
    class C(Document):
        y = RefField(DocumentField(A), autoload=True)


    a = A(x=5)
    s = get_session()
    s.insert(a)
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbref = DBRef(collection='A', id=a.mongo_id)

    b = B(y=a)
    assert b.wrap()['y'] == dbref

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
    b = B(y=a)
    s.insert(b)

    s2 = get_session(cache_size=10)
    b2 = s2.query(B).filter_by(mongo_id=b.mongo_id).one()
    assert id(s.dereference(b2.y)) == id(a)


def test_wrap_unwrap():
    class A(Document):
        x = IntField()
    class AA(Document):
        x = IntField()
    class B(Document):
        y = RefField(DocumentField(A))
    class C(Document):
        y = RefField(DocumentField(A), autoload=True)

    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbref = DBRef(database='unit-testing', collection='A', id=a.mongo_id)
    dbref_without_db = DBRef(collection='A', id=a.mongo_id)

    f = RefField(DocumentField(A))
    assert f.wrap(a) == f.wrap(f.unwrap(f.wrap(a))), (f.wrap(a), f.wrap(f.unwrap(f.wrap(a))))


def test_wrap():
    class A(Document):
        x = IntField()
    class AA(Document):
        x = IntField()
    class B(Document):
        y = RefField(DocumentField(A))
    class C(Document):
        y = RefField(DocumentField(A), autoload=True)

    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbref = DBRef(database='unit-testing', collection='A', id=a.mongo_id)
    dbref_without_db = DBRef(collection='A', id=a.mongo_id)

    f = RefField(DocumentField(A), simple=True)
    assert f.wrap(a) == a.mongo_id

    f = RefField(simple=True, collection='A')
    assert f.wrap(a.wrap()) == a.mongo_id

    f = RefField(collection='A')
    assert f.wrap(a.wrap()) == dbref_without_db, (f.wrap(a.wrap()), dbref)

    f = RefField(collection='A', db='unit-testing')
    assert f.wrap(a.wrap()) == dbref, (f.wrap(a.wrap()), dbref)

def test_unwrap():
    class A(Document):
        x = IntField()
    class AA(Document):
        x = IntField()
    class B(Document):
        y = RefField(DocumentField(A))
    class C(Document):
        y = RefField(DocumentField(A), autoload=True)

    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbaref = DBRef(db='unit-testing', collection='A', id=a.mongo_id)

    ret = RefField(DocumentField(A)).unwrap(dbaref)
    assert isinstance(ret, DBRef), ret

    ret = RefField(DocumentField(A), simple=True).unwrap(a.mongo_id)
    assert isinstance(ret, DBRef), ret

    field = RefField(DocumentField(A), db='unit-testing', simple=True, autoload=True)
    ret = field.unwrap(a.mongo_id, session=s)
    assert isinstance(ret, A), ret

    field = RefField(collection='A', db='unit-testing', autoload=True)
    ret = field.unwrap(dbaref, session=s)
    assert ret.mongo_id == a.mongo_id

@raises(BadValueException)
def test_unwrap_bad_type():
    class A(Document):
        x = IntField()
    class AA(Document):
        x = IntField()
    class B(Document):
        y = RefField(DocumentField(A))
    class C(Document):
        y = RefField(DocumentField(A), autoload=True)

    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbaref = DBRef(db='unit-testing', collection='A', id=a.mongo_id)

    ret = RefField(DocumentField(A)).unwrap(5)

@raises(BadValueException)
def test_unwrap_bad_type_extra():
    class A(Document):
        x = IntField()
    class AA(Document):
        x = IntField()
    class B(Document):
        y = RefField(DocumentField(A))
    class C(Document):
        y = RefField(DocumentField(A), autoload=True)

    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbaref = DBRef(db='unit-testing', collection='A', id=a.mongo_id)

    ret = RefField(DocumentField(A)).validate_unwrap(5)

def test_simple():
    class A(Document):
        x = IntField()
    a = A(x=5)
    s = get_session()
    s.insert(a)

    ref = DBRef(db='unit-testing', collection='A', id=a)
    RefField(A, simple=True).wrap(ref)


def test_blank():
    class A(Document):
        x = IntField()
    a = A(x=5)
    s = get_session()
    s.insert(a)
    ref = DBRef(db='unit-testing', collection='A', id=a)
    field = RefField(simple=True, collection='A', autoload=True)
    assert field.unwrap(a.mongo_id, session=s).x == 5
    
def test_blank2():
    class A(Document):
        x = IntField()
    a = A(x=5)
    s = get_session()
    s.insert(a)
    ref = DBRef(db='unit-testing', collection='A', id=a)
    field = RefField(autoload=True)
    assert field.wrap(a).collection == 'A'

def test_validate():
    RefField(simple=True).validate_unwrap(3)

@raises(BadValueException)
def test_missing():
    class A(Document):
        x = IntField()
    a = A(x=5)
    s = get_session()
    s.insert(a)
    # ref = DBRef(db='unit-testing', collection='A', id=)
    field = RefField(simple=True, collection='A', autoload=True)
    oid = ObjectId()
    assert field.unwrap(oid, session=s) == None
        

def test_document_with_ref():
    class A(Document):
        x = IntField()
    class C(Document):
        y = RefField(DocumentField(A), autoload=True)

    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    c = C(y=a)
    s.insert(c)
    for c in s.query(C).all():
        assert c.y.x == 5

@raises(BadValueException)
def test_document_with_error():
    class A(Document):
        x = IntField()
    class AA(Document):
        x = IntField()
    class B(Document):
        y = RefField(DocumentField(A))
    class C(Document):
        y = RefField(DocumentField(A), autoload=True)

    s = get_session()
    aa = AA(x=4)
    s.insert(aa)
    c = C(y=aa)
    s.insert(c)

@raises(BadValueException)
def test_unsaved_ref():
    class A(Document):
        x = IntField()
    class AA(Document):
        x = IntField()
    class B(Document):
        y = RefField(DocumentField(A))
    class C(Document):
        y = RefField(DocumentField(A), autoload=True)

    s = get_session()
    a = A(x=4)
    c = C(y=a)
    s.insert(c)




# @raises(BadFieldSpecification)
# def wrong_type_test():
#     class D(Document):
#         y = RefField()

@raises(BadFieldSpecification)
def collection_and_type_test():
    class A(Document):
        x = IntField()
    class AA(Document):
        x = IntField()
    class B(Document):
        y = RefField(DocumentField(A))
    class C(Document):
        y = RefField(DocumentField(A), autoload=True)

    class D(Document):
        y = RefField(DocumentField(A), collection='A')
