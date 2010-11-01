from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField, FieldNotRetrieved
from mongoalchemy.fields import *
from mongoalchemy.update_expression import InvalidModifierException
from mongoalchemy.query import BadQueryException, Query, BadResultException, RemoveQuery
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

# General Update tests

def test_multi():
    q = update_test_setup()
    q.set(T.f.i, 5).set(T.f.j, 6).upsert().execute()
    q.set(T.f.i, 5).set(T.f.j, 7).upsert().execute()
    q.set(T.f.i, 5).set(T.f.j, 8).upsert().execute()
    q.set(T.f.j, 9).multi().execute()
    for t in q:
        assert t.j == 9

# Test Remove

def test_remove():
    # setup
    s = get_session()
    s.clear_collection(T)
    for i in range(0, 15):
        s.insert(T(i=i))
    assert s.query(T).count() == 15
    
    def getall():
        return [t.i for t in s.query(T).all()]
    
    s.remove_query(T).filter(T.f.i > 8).execute()
    assert s.query(T).count() == 9
    
    # TODO: to /really/ test this I need to cause an error.
    remove_result = s.remove_query(T).filter(T.f.i > 7).set_safe(True).execute()
    assert remove_result['ok'] == 1
    assert s.query(T).count() == 8
    
    s.remove_query(T).or_(T.f.i == 7, T.f.i == 6).execute()
    remaining = [0, 1, 2, 3, 4, 5]
    assert remaining == getall(), getall()
    
    s.remove_query(T).in_(T.f.i, 0, 1).execute()
    remaining.remove(1)
    remaining.remove(0)
    assert remaining == getall(), getall()
    
    s.remove_query(T).nin(T.f.i, 2, 3, 4).execute()
    remaining.remove(5)
    assert remaining == getall(), getall()


def test_remove_obj():
    s = get_session()
    s.clear_collection(T)
    t = T(i=4)
    s.insert(t)
    assert s.query(T).count() == 1
    s.remove(t)
    assert s.query(T).count() == 0
    t2 = T(i=3)
    s.remove(t2)
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
