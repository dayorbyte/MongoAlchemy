from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index
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
    
    try:
        import json
    except:
        import simplejson as json
    
    desired = '''{"_id_": {"key": [["_id", 1]], "v": 1}, "int1_1_str3_-1": {"dropDups": false, "key": [["int1", 1], ["str3", -1]], "v": 1}, "str1_-1": {"dropDups": true, "key": [["str1", -1]], "unique": true, "v": 1}, "str2_-1": {"dropDups": false, "key": [["str2", -1]], "unique": true, "v": 1}, "str3_-1": {"dropDups": false, "key": [["str3", -1]], "v": 1}}'''
    got = s.get_indexes(TestDoc)
    got = json.dumps(got, sort_keys=True)
    assert got == desired, '\nG: %s\nD: %s' % (got, desired)

@known_failure
@raises(Exception)
def no_field_index_test():
    class TestDoc2(TestDoc):
        index_1 = Index().ascending('noexists')
    s.get_session()
    s.clear_collection(TestDoc)
    s.insert(t)

