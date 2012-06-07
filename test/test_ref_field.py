from nose.tools import *
from mongoalchemy.fields import *

from mongoalchemy.exceptions import DocumentException, MissingValueException, ExtraValueException, FieldNotRetrieved, BadFieldSpecification
from mongoalchemy.document import Document, DocumentField, document_type_registry
from mongoalchemy.session import Session
from test.util import known_failure
from datetime import datetime
from bson.dbref import DBRef

def get_session():
    s = Session.connect('unit-testing')
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

# Field Tests

def test_reffield():


    a = A(x=5)
    s = get_session()
    s.insert(a)
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbref = DBRef(collection='A', id=a.mongo_id)

    b = B(y=a)
    assert b.wrap()['y'] == dbref

def test_wrap():
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
    assert ret['_id'] == a.mongo_id

@raises(BadValueException)
def test_unwrap_bad_type():
    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbaref = DBRef(db='unit-testing', collection='A', id=a.mongo_id)

    ret = RefField(DocumentField(A)).unwrap(5)

@raises(BadValueException)
def test_unwrap_bad_type_extra():
    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    aref = {'$id':a.mongo_id, '$ref':'A'}
    dbaref = DBRef(db='unit-testing', collection='A', id=a.mongo_id)

    ret = RefField(DocumentField(A)).validate_unwrap(5)



def test_document_with_ref():
    s = get_session()
    
    a = A(x=5)
    s.insert(a)
    
    c = C(y=a)
    s.insert(c)
    for c in s.query(C).all():
        assert c.y.x == 5

@raises(BadValueException)
def test_document_with_error():
    s = get_session()
    aa = AA(x=4)
    s.insert(aa)
    c = C(y=aa)
    s.insert(c)

@raises(BadValueException)
def test_unsaved_ref():
    s = get_session()
    a = A(x=4)
    c = C(y=a)
    s.insert(c)




@raises(BadFieldSpecification)
def wrong_type_test():
    class D(Document):
        y = RefField(A)

@raises(BadFieldSpecification)
def collection_and_type_test():
    class D(Document):
        y = RefField(DocumentField(A), collection='A')
