from nose.tools import *
from pprint import pprint
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField, MissingValueException
from mongoalchemy.fields import *
from test.util import known_failure

# Document Types used in some tests
class TestDoc(Document):
    int1 = IntField()
    def __repr__(self):
        return 'TestDoc(int1=%d)' % self.int1

# Document Types used in some tests
class TestDoc2(Document):
    sfield = StringField()
    def __repr__(self):
        return 'TestDoc(int1=%s)' % self.sfield


class DocA(Document):
    test_doc = DocumentField(TestDoc, required=False)
    test_doc2 = DocumentField(TestDoc2, required=False)
    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        return self.test_doc.int1 == other.test_doc.int1
    def __repr__(self):
        return 'DocA()'

# Tests

def get_session():
    return Session.connect('unit-testing')

def test_basic():
    class Doc(Document):
        count = IntField()
    s = get_session()
    d = Doc(count=0)
    s.insert(d)
    assert d._id

def test_basic2():
    class Doc(Document):
        _collection_name = 'DocCol'
        count = IntField()
    
    assert Doc.class_name() == 'Doc', Doc.class_name()
    assert Doc.get_collection_name() == 'DocCol'

@raises(MissingValueException)
def test_required_fields():
    class Doc(Document):
        i = IntField()
    Doc().wrap()

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


def test_doc_field():
    
    sd = TestDoc(int1=0)
    doca = DocA(test_doc=sd)
    wrapped = doca.wrap()
    unwrapped = DocA.unwrap(wrapped)
    assert unwrapped == doca

@raises(BadValueException)
def wrong_wrap_type_test():
    doc1 = TestDoc(int1=0)
    doc2 = TestDoc2(sfield='a')
    doca = DocA(test_doc=doc2)
    doca.wrap()

@raises(BadValueException)
def wrong_wrap_type_test2():
    doc2 = TestDoc2(sfield=1) # this is an invalid value
    doca = DocA(test_doc2=doc2)
    doca.wrap()


@raises(BadValueException)
def wrong_unwrap_type_test():
    DocA.unwrap({ 'test_doc2' : { 'int1' : 1 } })


