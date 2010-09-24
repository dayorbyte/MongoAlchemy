from nose.tools import *
from pprint import pprint
from mongomapper.session import Session
from mongomapper.document import Document, Index, DocumentField
from mongomapper.fields import *
from test.util import known_failure

class TestDoc(Document):
    int1 = IntField()

def get_session():
    return Session.connect('unit-testing')

def test_basic():
    class Doc(Document):
        count = IntField()
    
    s = get_session()
    d = Doc(count=0)
    s.insert(d)
    assert d._id

@raises(Exception)
def bad_field_test():
    s = get_session()
    s.clear_collection(TestDoc)
    t = TestDoc(int1=1, str4='sdasa')

def loading_test():
    s = get_session()
    s.clear_collection(TestDoc)
    t = TestDoc(int1=123431)
    s.insert(t)
    for td in s.query(TestDoc):
        break
    assert td.int1 == t.int1

def docfield_test():
    class SuperDoc(Document):
        int1 = IntField()
        sub = DocumentField(TestDoc)
    
    s = get_session()
    s.clear_collection(TestDoc, SuperDoc)
    
    doc = TestDoc(int1=3)
    sup = SuperDoc(int1=4, sub=doc)
    
    s.insert(sup)
    
    for sd in s.query(SuperDoc):
        break
    
    assert sd.int1 == sup.int1
    assert sd.sub.int1 == doc.int1
    