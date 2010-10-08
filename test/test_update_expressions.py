from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField, FieldNotRetrieved
from mongoalchemy.fields import *
from mongoalchemy.update_expression import InvalidModifierException
from mongoalchemy.query import BadQueryException, Query, BadResultException
from test.util import known_failure

class T(Document):
    i = IntField()
    j = IntField(required=False)
    s = StringField(required=False)
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

# SET

def set_test():
    q = update_test_setup()
    assert q.set(T.f.i, 5).set(T.f.j, 7).update_data == {
        '$set' : { 'i' : 5, 'j' : 7 }
    }

def set_db_test():
    q = update_test_setup()
    
    q.set(T.f.i, 5).set(T.f.j, 6).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.j == 6
    
    q.filter(T.f.i == 5).set(T.f.j, 7).execute()
    t = q.one()
    assert t.i == 5 and t.j == 7


# UNSET

def unset_test():
    q = update_test_setup()
    assert q.unset(T.f.i).update_data == {
        '$unset' : { 'i' : True }
    }

def unset_db_test():
    q = update_test_setup()
    
    q.set(T.f.i, 5).set(T.f.j, 6).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.j == 6
    
    q.filter(T.f.i == 5).unset(T.f.j).execute()
    t = q.one()
    assert t.i == 5 and not hasattr(t, 'j')


# INC

def inc_test():
    q = update_test_setup()
    assert q.inc(T.f.i, 4).update_data == {
        '$inc' : { 'i' : 4 }
    }
    assert q.inc(T.f.i).inc(T.f.j).update_data == {
        '$inc' : { 'i' : 1, 'j' : 1 }
    }

@raises(InvalidModifierException)
def inc_invalid_test():
    q = update_test_setup()
    q.inc(T.f.s, 1)

def inc_db_test():
    q = update_test_setup()
    
    q.inc(T.f.i, 5).inc(T.f.j, 6).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.j == 6
    q.inc(T.f.j, 6).inc(T.f.i, 5).execute()
    t = q.one()
    assert t.i == 10 and t.j == 12

# APPEND

def append_test():
    q = update_test_setup()
    assert q.append(T.f.l, 1).update_data == {
        '$push' : { 'l' : 1 }
    }

@raises(InvalidModifierException)
def append_invalid_test():
    q = update_test_setup()
    q.append(T.f.s, 1)

def append_db_test():
    q = update_test_setup()
    
    q.set(T.f.i, 5).append(T.f.l, 6).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6]
    
    q.append(T.f.l, 5).execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5]


# EXTEND

def extend_test():
    q = update_test_setup()
    assert q.extend(T.f.l, *(1, 2, 3)).update_data == {
        '$pushAll' : { 'l' : (1, 2, 3) }
    }

def extend_db_test():
    q = update_test_setup()
    
    q.set(T.f.i, 5).extend(T.f.l, 6, 5).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5]
    
    q.extend(T.f.l, 4, 3).execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]

@raises(InvalidModifierException)
def extend_invalid_test():
    q = update_test_setup()
    q.extend(T.f.s, [1])

# REMOVE

def remove_test():
    q = update_test_setup()
    assert q.remove(T.f.l, 1).update_data == {
        '$pull' : { 'l' : 1 }
    }

@raises(InvalidModifierException)
def remove_invalid_test():
    q = update_test_setup()
    q.remove(T.f.s, 1)

def remove_db_test():
    q = update_test_setup()
    
    q.set(T.f.i, 5).extend(T.f.l, 6, 5, 4, 3).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]
    
    q.remove(T.f.l, 4).execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 3]


# REMOVE ALL

def remove_all_test():
    q = update_test_setup()
    assert q.remove_all(T.f.l, *(1, 2, 3)).update_data == {
        '$pullAll' : { 'l' : (1, 2, 3) }
    }

@raises(InvalidModifierException)
def remove_all_invalid_test():
    q = update_test_setup()
    q.remove_all(T.f.s, 1, 2, 3)

def remove_all_db_test():
    q = update_test_setup()
    
    q.set(T.f.i, 5).extend(T.f.l, 6, 5, 4, 3).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]
    
    q.remove_all(T.f.l, 6, 5, 4).execute()
    t = q.one()
    assert t.i == 5 and t.l == [3]


# ADD TO SET

def add_to_set_test():
    q = update_test_setup()
    assert q.add_to_set(T.f.l, 1).update_data == {
        '$addToSet' : { 'l' : 1 }
    }

def add_to_set_db_test():
    q = update_test_setup()
    
    q.set(T.f.i, 5).extend(T.f.l, 6, 5, 4, 3).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]
    
    q.add_to_set(T.f.l, 6).add_to_set(T.f.l, 2).execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3, 2]

@raises(InvalidModifierException)
def add_to_set_invalid_test():
    q = update_test_setup()
    q.add_to_set(T.f.s, 1)

# POP FIRST

def pop_first_test():
    q = update_test_setup()
    assert q.pop_first(T.f.l).update_data == {
        '$pop' : { 'l' : -1 }
    }

@raises(InvalidModifierException)
def pop_first_invalid_test():
    q = update_test_setup()
    q.pop_last(T.f.s)

def pop_first_db_test():
    q = update_test_setup()
    
    q.set(T.f.i, 5).extend(T.f.l, 6, 5, 4, 3).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]
    
    q.pop_first(T.f.l).execute()
    t = q.one()
    assert t.i == 5 and t.l == [5, 4, 3]


# POP LAST

def pop_last_test():
    q = update_test_setup()
    assert q.pop_last(T.f.l).update_data == {
        '$pop' : { 'l' : 1 }
    }


@raises(InvalidModifierException)
def pop_last_invalid_test():
    q = update_test_setup()
    q.pop_first(T.f.s)

def pop_last_db_test():
    q = update_test_setup()
    
    q.set(T.f.i, 5).extend(T.f.l, 6, 5, 4, 3).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]
    
    q.pop_last(T.f.l).execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4]
