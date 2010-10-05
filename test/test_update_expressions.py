from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField, FieldNotRetrieved
from mongoalchemy.fields import *
from mongoalchemy.query import BadQueryException, Query, BadResultException
from test.util import known_failure

class T(Document):
    i = IntField()
    j = IntField(required=False)
    l = ListField(IntField(), required=False)
    a = IntField(required=False, db_field='aa')
    index = Index().ascending('i')

class T2(Document):
    t = DocumentField(T)

def get_session():
    return Session.connect('unit-testing')

#
#   Update Operation Tests
#

def update_test_setup():
    s = get_session()
    s.clear_collection(T)
    return s.query(T)


def set_test():
    q = update_test_setup()
    assert q.set(T.f.i, 5).update_data == {
        '$set' : { 'i' : 5 }
    }

def unset_test():
    q = update_test_setup()
    assert q.unset(T.f.i).update_data == {
        '$unset' : { 'i' : True }
    }
    
def inc_test():
    q = update_test_setup()
    assert q.inc(T.f.i, 4).update_data == {
        '$inc' : { 'i' : 4 }
    }
    assert q.inc(T.f.i).update_data == {
        '$inc' : { 'i' : 1 }
    }
    
    
def append_test():
    q = update_test_setup()
    assert q.append(T.f.l, 1).update_data == {
        '$push' : { 'l' : 1 }
    }
    
def extend_test():
    q = update_test_setup()
    assert q.extend(T.f.l, *(1, 2, 3)).update_data == {
        '$pushAll' : { 'l' : (1, 2, 3) }
    }

def remove_test():
    q = update_test_setup()
    assert q.remove(T.f.l, 1).update_data == {
        '$pull' : { 'l' : 1 }
    }
    
def remove_all_test():
    q = update_test_setup()
    assert q.remove_all(T.f.l, *(1, 2, 3)).update_data == {
        '$pullAll' : { 'l' : (1, 2, 3) }
    }


def add_to_set_test():
    q = update_test_setup()
    assert q.add_to_set(T.f.l, 1).update_data == {
        '$addToSet' : { 'l' : 1 }
    }
    
def pop_first_test():
    q = update_test_setup()
    assert q.pop_first(T.f.l).update_data == {
        '$pop' : { 'l' : -1 }
    }

def pop_last_test():
    q = update_test_setup()
    assert q.pop_last(T.f.l).update_data == {
        '$pop' : { 'l' : 1 }
    }



