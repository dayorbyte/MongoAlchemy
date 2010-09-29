from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField
from mongoalchemy.fields import *
from test.util import known_failure

def get_session():
    return Session.connect('unit-testing')

class TestDoc(Document):
    
    int1 = IntField()
    str1 = StringField()
    str2 = StringField()
    str3 = StringField()
    
    index_1 = Index().ascending('int1').descending('str3')
    index_2 = Index().descending('str3')
    index_3 = Index().descending('str2').unique()
    index_4 = Index().descending('str1').unique(drop_dups=True)

def test_indexes():
    s = get_session()
    s.clear_collection(TestDoc)
    t = TestDoc(int1=1, str1='a', str2='b', str3='c')
    s.insert(t)
    
    assert s.get_indexes(TestDoc) == {u'_id_': {u'key': [(u'_id', 1)]},
         u'int1_1_str3_-1': {u'dropDups': False,
                             u'key': [(u'int1', 1), (u'str3', -1)],
                             u'unique': False},
         u'str1_-1': {u'dropDups': True, u'key': [(u'str1', -1)], u'unique': True},
         u'str2_-1': {u'dropDups': False, u'key': [(u'str2', -1)], u'unique': True},
         u'str3_-1': {u'dropDups': False, u'key': [(u'str3', -1)], u'unique': False}}

@known_failure
@raises(Exception)
def no_field_index_test():
    class TestDoc2(TestDoc):
        index_1 = Index().ascending('noexists')
    s.get_session()
    s.clear_collection(TestDoc)
    s.insert(t)

