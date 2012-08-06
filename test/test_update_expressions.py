from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, FieldNotRetrieved
from mongoalchemy.fields import *
from mongoalchemy.update_expression import InvalidModifierException, UpdateException
from mongoalchemy.query import BadQueryException, Query, BadResultException, RemoveQuery
from test.util import known_failure
from pymongo.errors import DuplicateKeyError

class T(Document):
    i = IntField()
    j = IntField(required=False)
    s = StringField(required=False)
    l = ListField(IntField(), required=False)
    a = IntField(required=False, db_field='aa')
    index = Index().ascending('i')

class T2(Document):
    t = DocumentField(T)

class TUnique(Document):
    i = IntField()
    j = IntField(required=False)
    main_index = Index().ascending('i').unique()


def get_session():
    return Session.connect('unit-testing')

#
#   Update Operation Tests
#

def update_test_setup():
    s = get_session()
    s.clear_collection(T)
    return s.query(T)

# Find and Modify
def test_find_and_modify():
    s = get_session()
    s.clear_collection(T, T2)
    # insert
    value = s.query(T).filter_by(i=12341).find_and_modify().set(i=12341).upsert().execute()
    assert value == {}, value
    assert s.query(T).one().i == 12341
    
    # update
    value = s.query(T).filter_by(i=12341).ascending(T.i).find_and_modify().set(i=9999).execute()
    assert value.i == 12341
    assert s.query(T).one().i == 9999
    
    # return new
    value = s.query(T).filter_by(i=9999).fields(T.i).find_and_modify(new=True).set(i=8888).execute()
    assert value.i == 8888, value.i
    assert s.query(T).one().i == 8888

    # remove
    value = s.query(T).filter_by(i=8888).find_and_modify(remove=True).execute()
    assert value.i == 8888, value.i
    assert s.query(T).first() is None

    # update
    value = s.query(T).filter_by(i=1000).ascending(T.i).find_and_modify(new=True).set(i=0).execute()
    assert value is None, value


# General Update tests

def test_multi():
    q = update_test_setup()
    q.set(T.i, 5).set(T.j, 6).upsert().execute()
    q.set(T.i, 5).set(T.j, 7).upsert().execute()
    q.set(T.i, 5).set(T.j, 8).upsert().execute()
    q.set(T.j, 9).multi().execute()
    for t in q:
        assert t.j == 9

# Test Nested object
def nested_field_set_test():
    s = get_session()
    s.clear_collection(T, T2)
    s.query(T2).set('t.i', 3).upsert().execute()
    assert s.query(T2).one().t.i == 3


def test_update_safe():
    s = get_session()
    s.clear_collection(TUnique)
    s.query(TUnique).filter_by(i=1).set(i=1, j=2).upsert().execute()
    # default safe=false -- ignore error
    s.query(TUnique).filter_by(i=1).set(i=1, j=2).upsert().execute()
    # explicit safe=false
    s.query(TUnique).filter_by(i=1).set(i=1, j=2).safe().safe(safe=False).upsert().execute()
    
    # safe=true, exception
    # TODO: doesn't produce a real exception.  should investigate why, but I checked
    # and I am sending safe=True
    # 
    # try:
    #     s.query(TUnique).filter_by(i=1).set(i=1, j=2).safe().upsert().execute()
    #     assert False, 'No error raised on safe insert for second unique item'
    # except DuplicateKeyError:
    #     pass


# Test Remove

def test_remove():
    # setup
    s = get_session()
    s.clear_collection(T)
    for i in range(0, 15):
        s.insert(T(i=i))
    assert s.query(T).count() == 15
    
    def getall():
        return [t.i for t in s.query(T).ascending(T.i).all()]
    
    s.remove_query(T).filter(T.i > 8).execute()
    assert s.query(T).count() == 9
    
    # TODO: to /really/ test this I need to cause an error.
    remove_result = s.remove_query(T).filter(T.i > 7).set_safe(True).execute()
    assert remove_result['ok'] == 1
    assert s.query(T).count() == 8
    
    s.remove_query(T).or_(T.i == 7, T.i == 6).execute()
    remaining = [0, 1, 2, 3, 4, 5]
    assert remaining == getall(), getall()
    
    s.remove_query(T).in_(T.i, 0, 1).execute()
    remaining.remove(1)
    remaining.remove(0)
    assert remaining == getall(), getall()
    
    s.remove_query(T).nin(T.i, 2, 3, 4).execute()
    remaining.remove(5)
    assert remaining == getall(), getall()
    
    s.remove_query(T).filter_by(i=2).execute()
    remaining.remove(2)
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

def test_remove_expression():
    class Page(Document):
        text = StringField()
        page_num = IntField()

    class Book(Document):
        isbn = StringField()
        pages = ListField(DocumentField(Page))
    
    session = get_session()
    session.clear_collection(Book)

    ISBN = '3462784290890'
    book = Book(isbn=ISBN, pages=[])
    for i in range(0,10):
        book.pages.append(Page(text='p%d' % i, page_num = i))

    session.insert(book)

    query = session.query(Book).filter(Book.isbn == ISBN, Book.pages.page_num > 5)
    query.remove(Book.pages, Page.page_num == 8).execute()
    book = session.query(Book).filter(Book.isbn == ISBN).one()
    assert len(book.pages) == 9

@raises(InvalidModifierException)
def test_remove_expression_error():
    class Page(Document):
        text = StringField()
        page_num = IntField()
    
    class Book(Document):
        isbn = StringField()
        pages = ListField(DocumentField(Page))
    session = get_session()
    session.query(Book).remove(Book.isbn, Page.page_num == 8)


# SET

def set_test():
    q = update_test_setup()
    assert q.set(T.i, 5).set(T.j, 7).update_data == {
        '$set' : { 'i' : 5, 'j' : 7 }
    }

def set_test_kwargs():
    q = update_test_setup()
    assert q.set(i=5, j=7).update_data == {
        '$set' : { 'i' : 5, 'j' : 7 }
    }

def set_db_test():
    q = update_test_setup()
    
    q.set(T.i, 5).set(T.j, 6).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.j == 6
    
    q.filter(T.i == 5).set(T.j, 7).execute()
    t = q.one()
    assert t.i == 5 and t.j == 7


def set_db_test_kwargs():
    q = update_test_setup()
    
    q.set(i=5, j=6).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.j == 6
    
    q.filter_by(i=5).set(j=7).execute()
    t = q.one()
    assert t.i == 5 and t.j == 7

@raises(UpdateException)
def set_bad_args_test_1():
    q = update_test_setup()
    q.set()

@raises(UpdateException)
def set_bad_args_test_2():
    q = update_test_setup()
    q.set(1, 2, 3)


# UNSET

def unset_test():
    q = update_test_setup()
    assert q.unset(T.i).update_data == {
        '$unset' : { 'i' : True }
    }

def unset_db_test():
    q = update_test_setup()
    
    q.set(T.i, 5).set(T.j, 6).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.j == 6
    
    ue = q.filter(T.i == 5).unset(T.j).execute()
    t = q.one()
    assert t.i == 5 and not hasattr(t, 'j'), getattr(t, 'j')


# INC

def inc_test():
    q = update_test_setup()
    assert q.inc(T.i, 4).update_data == {
        '$inc' : { 'i' : 4 }
    }
    assert q.inc(T.i).inc(T.j).update_data == {
        '$inc' : { 'i' : 1, 'j' : 1 }
    }
    assert q.inc(i=1, j=1).update_data == {
        '$inc' : { 'i' : 1, 'j' : 1 }
    }

@raises(UpdateException)
def test_inc_bad_value():
    update_test_setup().inc().update_data

@raises(InvalidModifierException)
def inc_invalid_test():
    q = update_test_setup()
    q.inc(T.s, 1)

def inc_db_test():
    q = update_test_setup()
    
    q.inc(T.i, 5).inc(T.j, 6).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.j == 6
    q.inc(T.j, 6).inc(T.i, 5).execute()
    t = q.one()
    assert t.i == 10 and t.j == 12

# APPEND

def append_test():
    q = update_test_setup()
    assert q.append(T.l, 1).update_data == {
        '$push' : { 'l' : 1 }
    }

@raises(InvalidModifierException)
def append_invalid_test():
    q = update_test_setup()
    q.append(T.s, 1)

def append_db_test():
    q = update_test_setup()
    
    q.set(T.i, 5).append(T.l, 6).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6]
    
    q.append(T.l, 5).execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5]


# EXTEND

def extend_test():
    q = update_test_setup()
    assert q.extend(T.l, *(1, 2, 3)).update_data == {
        '$pushAll' : { 'l' : (1, 2, 3) }
    }

def extend_db_test():
    q = update_test_setup()
    
    q.set(T.i, 5).extend(T.l, 6, 5).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5]
    
    q.extend('l', 4, 3).execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]

@raises(InvalidModifierException)
def extend_invalid_test():
    q = update_test_setup()
    q.extend(T.s, [1])

# REMOVE

def remove_test():
    q = update_test_setup()
    assert q.remove(T.l, 1).update_data == {
        '$pull' : { 'l' : 1 }
    }

@raises(InvalidModifierException)
def remove_invalid_test():
    q = update_test_setup()
    q.remove(T.s, 1)

def remove_db_test():
    q = update_test_setup()
    
    q.set(T.i, 5).extend(T.l, 6, 5, 4, 3).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]
    
    q.remove(T.l, 4).execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 3]


# REMOVE ALL

def remove_all_test():
    q = update_test_setup()
    assert q.remove_all(T.l, *(1, 2, 3)).update_data == {
        '$pullAll' : { 'l' : (1, 2, 3) }
    }

@raises(InvalidModifierException)
def remove_all_invalid_test():
    q = update_test_setup()
    q.remove_all(T.s, 1, 2, 3)

def remove_all_db_test():
    q = update_test_setup()
    
    q.set(T.i, 5).extend(T.l, 6, 5, 4, 3).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]
    
    q.remove_all(T.l, 6, 5, 4).execute()
    t = q.one()
    assert t.i == 5 and t.l == [3]


# ADD TO SET

def add_to_set_test():
    q = update_test_setup()
    assert q.add_to_set(T.l, 1).update_data == {
        '$addToSet' : { 'l' : 1 }
    }

def add_to_set_db_test():
    q = update_test_setup()
    
    q.set(T.i, 5).extend(T.l, 6, 5, 4, 3).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]
    
    q.add_to_set(T.l, 6).add_to_set(T.l, 2).execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3, 2]

@raises(InvalidModifierException)
def add_to_set_invalid_test():
    q = update_test_setup()
    q.add_to_set(T.s, 1)

# POP FIRST

def pop_first_test():
    q = update_test_setup()
    assert q.pop_first(T.l).update_data == {
        '$pop' : { 'l' : -1 }
    }

@raises(InvalidModifierException)
def pop_first_invalid_test():
    q = update_test_setup()
    q.pop_last(T.s)

def pop_first_db_test():
    q = update_test_setup()
    
    q.set(T.i, 5).extend(T.l, 6, 5, 4, 3).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]
    
    q.pop_first(T.l).execute()
    t = q.one()
    assert t.i == 5 and t.l == [5, 4, 3]


# POP LAST

def pop_last_test():
    q = update_test_setup()
    assert q.pop_last(T.l).update_data == {
        '$pop' : { 'l' : 1 }
    }


@raises(InvalidModifierException)
def pop_last_invalid_test():
    q = update_test_setup()
    q.pop_first(T.s)

def pop_last_db_test():
    q = update_test_setup()
    
    q.set(T.i, 5).extend(T.l, 6, 5, 4, 3).upsert().execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4, 3]
    
    q.pop_last(T.l).execute()
    t = q.one()
    assert t.i == 5 and t.l == [6, 5, 4]

def test_update_obeys_db_field():
    '''Updating a document should update the db_field in the \
    database'''
    s = get_session()

    s.db.Foo.remove()
    class Foo(Document):
        mongo_id = IntField(db_field='_id')
        bar = StringField(db_field='baz')

    a = Foo(mongo_id=2, bar="Hello")
    s.update(a, upsert=True, safe=True)

    a_from_db = s.db.Foo.find_one()
    assert a_from_db is not None
    assert 'mongo_id' not in a_from_db
    assert 'bar' not in a_from_db
    assert a_from_db['_id'] == 2
    assert a_from_db['baz'] == u'Hello'

    s.db.Foo.remove()
