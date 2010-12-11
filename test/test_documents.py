from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField, MissingValueException, DocumentException, DictDoc
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

class T(Document, DictDoc):
    i = IntField()
    j = IntField(required=False)
    s = StringField(required=False)
    l = ListField(IntField(), required=False)
    a = IntField(required=False, db_field='aa')
    index = Index().ascending('i')


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
    assert d.mongo_id

def test_basic2():
    class Doc(Document):
        config_collection_name = 'DocCol'
        count = IntField()
    
    assert Doc.class_name() == 'Doc', Doc.class_name()
    assert Doc.get_collection_name() == 'DocCol'

def test_update_ops():
    td = TestDoc(int1=1)
    doca = DocA(test_doc=td)
    assert doca.get_dirty_ops() == {
        '$set' : { 'test_doc.int1' : 1 }
    }, doca.get_dirty_ops()
    
    class DocB(Document):
        a = DocumentField(DocA)
        b = IntField()
    assert DocB(a=DocA()).get_dirty_ops() == {}

def test_delete_field():
    class Doc(Document):
        a = IntField()
        b = IntField()
    
    d = Doc()
    d.a = 5
    assert d.a == 5
    del d.a
    try:
        b = d.a
        assert False, 'delete attribute a failed'
    except AttributeError:
        pass

    try:
        del d.b
        assert False, 'delete attribute b failed'
    except AttributeError:
        pass
        
    

@raises(DocumentException)
def bad_extra_fields_param_test():
    class BadDoc(Document):
        config_extra_fields = 'blah'

def extra_fields_test():
    class BadDoc(Document):
        config_extra_fields = 'ignore'
    doc_with_extra = {'foo' : [1]}
    
    unwrapped = BadDoc.unwrap(doc_with_extra)
    assert unwrapped.get_extra_fields() == doc_with_extra
    
    assert BadDoc.wrap(unwrapped) == doc_with_extra


@raises(MissingValueException)
def test_required_fields():
    class Doc(Document):
        i = IntField()
    Doc().wrap()

@raises(AttributeError)
def test_missing_fields():
    class Doc(Document):
        i = IntField(required=False)
    Doc().i

def test_non_existant_field():
    class Doc(Document):
        i = IntField(required=False)
    Doc().j = 5
    


def test_default_value():
    class Doc(Document):
        i = IntField(required=False, default=1)
    assert Doc().i == 1

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

def test_doc_field_with_alternate_name():
    class Doc(Document):
        i = IntField(db_field='ii')
        def __eq__(self, other):
            return self.i == other.i
    d = Doc(i=3)
    wrapped = d.wrap()
    assert wrapped == {'ii' : 3}
    assert d == Doc.unwrap({'ii' : 3})

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

def is_valid_unwrap_test_true():
    assert DocA.test_doc.is_valid_unwrap({ 'int1' : 1 }) == True

def is_valid_unwrap_test_false():
    assert DocA.test_doc2.is_valid_unwrap({ 'int1' : 1 }) == False

@raises(BadValueException) 
def wrong_unwrap_type_test():
    DocA.unwrap({ 'test_doc2' : { 'int1' : 1 } })

# test DictDoc

def test_dictdoc_contains():
    t = T(i=1, retrieved_fields=[T.i, T.j])
    assert 'i' in t
    assert 'j' not in t
    assert 's' not in t
    assert 'noexist' not in t
    assert t['i'] == 1
        
def test_dictdoc_set():
    t = T(i=1, retrieved_fields=[T.i, T.j])
    assert 'i' in t
    t['i'] = 4
    assert t.i == 4

def test_dictdoc_setdefault():
    t = T(i=1, retrieved_fields=[T.i, T.j])
    
    assert t.setdefault('i', 4) == 1
    assert t.setdefault('j', 3) == 3
    