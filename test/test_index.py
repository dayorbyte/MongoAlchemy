from __future__ import print_function
from mongoalchemy.py3compat import *

import pymongo
from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index
from mongoalchemy.fields import *
from datetime import datetime
from test.util import known_failure
from time import sleep

PYMONGO_3 = pymongo.version_tuple >= (3, 0, 0)

def get_session():
    return Session.connect('unit-testing')

class TestDoc(Document):

    int1 = IntField()
    str1 = StringField()
    str2 = StringField()
    str3 = StringField(db_field='str_3_db_name')

    index_1 = Index().ascending(int1).descending(str3)
    index_2 = Index().descending(str3)
    index_3 = Index().descending('str2').unique()
    index_4 = Index().descending('str1').unique()

def test_indexes():
    s = get_session()
    s.db.drop_collection(TestDoc.get_collection_name())
    t = TestDoc(int1=1, str1='a', str2='b', str3='c')
    s.save(t)

    try:
        import json
    except:
        import simplejson as json

    if PYMONGO_3:
        desired = '''{"_id_": {"key": [["_id", 1]], "ns": "unit-testing.TestDoc", "v": 1}, "int1_1_str_3_db_name_-1": {"key": [["int1", 1], ["str_3_db_name", -1]], "ns": "unit-testing.TestDoc", "v": 1}, "str1_-1": {"key": [["str1", -1]], "ns": "unit-testing.TestDoc", "unique": true, "v": 1}, "str2_-1": {"key": [["str2", -1]], "ns": "unit-testing.TestDoc", "unique": true, "v": 1}, "str_3_db_name_-1": {"key": [["str_3_db_name", -1]], "ns": "unit-testing.TestDoc", "v": 1}}'''
    else:
        desired = '''{"_id_": {"key": [["_id", 1]], "v": 1}, "int1_1_str_3_db_name_-1": {"key": [["int1", 1], ["str_3_db_name", -1]], "v": 1}, "str1_-1": {"key": [["str1", -1]], "unique": true, "v": 1}, "str2_-1": {"key": [["str2", -1]], "unique": true, "v": 1}, "str_3_db_name_-1": {"key": [["str_3_db_name", -1]], "v": 1}}'''
    desired = json.dumps(json.loads(desired), sort_keys=True)
    got = s.get_indexes(TestDoc)
    got = json.dumps(got, sort_keys=True)
    assert got == desired, '\nG: %s\nD: %s' % (got, desired)

def expire_index_test():
    import os
    if os.environ.get('FAST_TESTS') == 'true':
        return
    class TestDoc3(TestDoc):
        date = DateTimeField()

        index_exire = Index().ascending('date').expire(30)
    t = TestDoc3(int1=123456, str1='abcd', str2='b', str3='c', date=datetime.utcnow())
    s = get_session()
    s.save(t)

    # Check that the document is indeed inserted
    assert len(s.query('TestDoc3').filter({'int1': 123456}).all()) > 0

    # The document will be deleted within a minute from its expiration
    # datetime
    sleep(62)

    # Check that the document is no longer there
    assert len(s.query('TestDoc3').filter({'int1': 123456}).all()) == 0


@known_failure
@raises(Exception)
def no_field_index_test():
    class TestDoc2(TestDoc):
        index_1 = Index().ascending('noexists')
    s.get_session()
    s.clear_collection(TestDoc)
    s.save(t)
