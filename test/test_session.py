from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index
from mongoalchemy.fields import *
from mongoalchemy.exceptions import *
from test.util import known_failure
from pymongo.errors import DuplicateKeyError

class T(Document):
    i = IntField()
    l = ListField(IntField(), required=False, on_update='$pushAll')

class TUnique(Document):
    i = IntField()
    main_index = Index().ascending('i').unique()

class TExtra(Document):
    i = IntField()
    config_extra_fields = 'ignore'

class TExtraDocField(Document):
    doc = DocumentField(TExtra)

class TExtraDocFieldList(Document):
    doclist = ListField(DocumentField(TExtra))

class TIntListDoc(Document):
    intlist = ListField(IntField())

def test_tz():
    import pytz
    from datetime import datetime
    class DT(Document):
        dt = DateTimeField()
        created = CreatedField(tz_aware=True)
        modified = ModifiedField(tz_aware=True)
        created1 = CreatedField(tz_aware=False)
        modified1 = ModifiedField(tz_aware=False)
    
    session = Session.connect('unit-testing', timezone=pytz.utc)
    assert session.tz_aware
    session.clear_collection(DT)
    d = DT(dt=pytz.utc.localize(datetime(2012, 1, 1)))
    assert d.created1.tzinfo is None
    assert d.modified1.tzinfo is None

    session.insert(d)
    for x in session.query(DT):
        assert x.dt.tzinfo is not None
        assert x.created.tzinfo is not None
        assert x.modified.tzinfo is not None

@raises(TransactionException)
def test_find_and_modify_in_session():
    s = Session.connect('unit-testing')
    with s:
        s.execute_find_and_modify({})

def test_session():
    s = Session.connect('unit-testing')
    s.clear_collection(T)
    s.insert(T(i=1))
    s.clear_queue()
    s.end()

def test_context_manager():
    with Session.connect('unit-testing') as s:
        s.clear_collection(T)
        t = T(i=5)

def test_safe():
    s = Session.connect('unit-testing', safe=True)
    assert s.safe == True
    s = Session.connect('unit-testing', safe=False)
    assert s.safe == False

def test_cache():
    s = Session.connect('unit-testing', cache_size=10)
    t = TExtra(i=4)
    s.insert(t)
    s.insert(t)
    t2 = s.query(TExtra).filter_by(mongo_id=t.mongo_id).one()
    assert id(t) == id(t2)
    assert id(s.refresh(t)) != t2

def test_cache2():
    s = Session.connect('unit-testing', cache_size=10)
    t = TExtra(i=4)
    s.insert(t)
    s.insert(t)
    for t2 in s.query(TExtra).filter_by(mongo_id=t.mongo_id):
        assert id(t) == id(t2)
        assert id(s.refresh(t)) != t2
        break
def test_cache3():
    s = Session.connect('unit-testing', cache_size=10)
    t = TExtra(i=4)
    s.insert(t)
    s.insert(t)
    t2 = s.query(TExtra).filter_by(mongo_id=t.mongo_id)[0]
    assert id(t) == id(t2)
    assert id(s.refresh(t)) != t2

# def test_cache2():
#     class SimpleDoc(Document):
#         i = IntField()

#     class CacheList(Document):
#         l_ids = ListField(RefField(SimpleDoc)
#     s = Session.connect('unit-testing', cache_size=10)
    
    

#     t2 = s.query(TExtra).filter_by(mongo_id=t.mongo_id).one()
#     assert id(t) == id(t2)
#     assert id(s.refresh(t)) != t2


def test_clone():
    s = Session.connect('unit-testing', cache_size=10)
    t = TExtra(i=4)
    s.insert(t)

    t2 = s.clone(t)
    s.insert(t2)

    assert t2.mongo_id != t.mongo_id
    

def test_cache_miss():
    s_nocache = Session.connect('unit-testing', cache_size=10)
    t = TExtra(i=4)
    s_nocache.insert(t)

    s = Session.connect('unit-testing', cache_size=10)
    s.add_to_session(t)
    t2 = s.query(TExtra).filter_by(mongo_id=t.mongo_id).one()
    # assert id(t) == id(t2)

def test_transactions():
    class Doc(Document):
        i = IntField()
    s = Session.connect('unit-testing')
    s.clear_collection(Doc)
    assert s.query(Doc).count() == 0
    with s:
        assert s.query(Doc).count() == 0
        s.add(Doc(i=4))
        assert s.query(Doc).count() == 0
        with s:
            assert s.query(Doc).count() == 0
            s.add(Doc(i=2))
            assert s.query(Doc).count() == 0
        assert s.query(Doc).count() == 0, s.query(Doc).count()
    assert s.query(Doc).count() == 2

def test_transactions2():
    class Doc(Document):
        i = IntField()
    s = Session.connect('unit-testing')
    s.clear_collection(Doc)
    assert s.query(Doc).count() == 0
    try:
        with s:
            assert s.query(Doc).count() == 0
            s.add(Doc(i=4))
            assert s.query(Doc).count() == 0
            with s:
                assert s.query(Doc).count() == 0
                s.add(Doc(i=2))
                assert s.query(Doc).count() == 0
                raise Exception()
            assert s.query(Doc).count() == 0, s.query(Doc).count()
    except:
        assert s.query(Doc).count() == 0, s.query(Doc).count()

def test_transactions3():
    class Doc(Document):
        i = IntField()
    s = Session.connect('unit-testing')
    s.clear_collection(Doc)
    assert s.query(Doc).count() == 0
    with s:
        s.add(Doc(i=4))
        try:

            with s:
                s.add(Doc(i=2))
                print 'RAISE'
                raise Exception()
        except:
            print 'CAUGHT'
            assert s.query(Doc).count() == 0, s.query(Doc).count()
    assert s.query(Doc).count() == 1, s.query(Doc).count()


def test_cache_max():
    # not a great test, but gets coverage
    s = Session.connect('unit-testing', cache_size=3)
    for i in range(0, 10):
        t = TExtra(i=4)
        s.insert(t)
    assert len(s.cache) == 3

def test_cache2():
    s = Session.connect('unit-testing')
    t = TExtra(i=4)
    s.insert(t)
    t2 = s.query(TExtra).filter_by(mongo_id=t.mongo_id).one()
    assert id(t) != id(t2)

def test_safe_with_error():
    s = Session.connect('unit-testing')
    s.clear_collection(TUnique)
    s.insert(TUnique(i=1))
    try:
        s.insert(TUnique(i=1), safe=True)
        assert False, 'No error raised on safe insert for second unique item'
    except DuplicateKeyError:
        assert len(s.queue) == 0
    

def test_update():
    s = Session.connect('unit-testing')
    s.clear_collection(T)
    t = T(i=6)
    s.insert(t)
    assert s.query(T).one().i == 6

    t.i = 7
    s.update(t)
    assert s.query(T).one().i == 7


def test_update_change_ops():
    s = Session.connect('unit-testing')
    s.clear_collection(T)
    t = T(i=6, l=[8])
    s.insert(t)
    assert s.query(T).one().i == 6

    t.i = 7
    t.l = [8]
    s.update(t, update_ops={T.l:'$pullAll'}, i='$inc')
    t = s.query(T).one()
    assert t.i == 13, t.i
    assert t.l == [], t.l

def test_update_push():
    s = Session.connect('unit-testing')
    s.clear_collection(T)
    # Create object
    t = T(i=6, l=[3])
    s.insert(t)
    t = s.query(T).one()
    assert t.i == 6 and t.l == [3]

    t = s.query(T).fields(T.i).one()
    t.i = 7
    t.l = [4]
    s.update(t, id_expression=T.i == 6)
    
    t = s.query(T).one()
    assert s.query(T).one().i == 7 and t.l == [3, 4]
    
def test_update_ignore_extras():
    s = Session.connect('unit-testing')
    s.clear_collection(TExtra)
    # Create Object
    t = TExtra(i=1, j='test', k='test2')
    s.insert(t)
    # Retrieve Object
    t = s.query(TExtra).one()
    assert t.i == 1
    assert t.get_extra_fields()['j'] == 'test'
    assert t.get_extra_fields()['k'] == 'test2'
    # Update Object
    t.i = 5
    del t.get_extra_fields()['j'] # delete an extra field
    t.get_extra_fields()['k'] = 'changed' # change an extra field
    t.get_extra_fields()['l'] = 'added' # new extra field
    s.update(t)

    # Retrieve Object
    t_new = s.query(TExtra).one()

    assert 'j' not in t_new.get_extra_fields()
    assert t_new.get_extra_fields()['k'] == 'changed'
    assert t_new.get_extra_fields()['l'] == 'added'
    assert t_new.i == 5


def test_update_docfield_extras():
    s = Session.connect('unit-testing')
    s.clear_collection(TExtraDocField)
    # Create Object
    t = TExtra(i=1, j='test')
    t2 = TExtraDocField(doc=t)
    s.insert(t2)
    # Retrieve Object
    t2 = s.query(TExtraDocField).one()
    assert t2.doc.i == 1
    assert t2.doc.get_extra_fields()['j'] == 'test'
    # Update Object's extra fields
    t2.doc.get_extra_fields()['t'] = 'added'

    s.update(t2)

    # Retrieve Object
    t2_new = s.query(TExtraDocField).one()
    assert t2_new.doc.i == 1
    assert t2_new.doc.get_extra_fields()['j'] == 'test'
    assert t2_new.doc.get_extra_fields()['t'] == 'added'

def test_update_docfield_list_extras():
    s = Session.connect('unit-testing')
    s.clear_collection(TExtraDocFieldList)

    # Create Objects
    t = TExtra(i=1, j='test')
    t2 = TExtra(i=2, j='test2')
    tListDoc = TExtraDocFieldList(doclist=[t, t2])

    s.insert(tListDoc)
    # Retrieve Object
    tListDoc = s.query(TExtraDocFieldList).one()
    assert len(tListDoc.doclist) == 2
    for doc in tListDoc.doclist:
        if doc.i == 1:
            assert doc.get_extra_fields()['j'] == 'test'
            # go ahead and update j now
            doc.get_extra_fields()['j'] = 'testChanged'
        elif doc.i == 2:
            assert doc.get_extra_fields()['j'] == 'test2'
        else:
            assert False

    # update the parent document
    s.update(tListDoc)

    # re-fetch and verify
    tListDoc = s.query(TExtraDocFieldList).one()

    for doc in tListDoc.doclist:
        if doc.i == 1:
            assert doc.get_extra_fields()['j'] == 'testChanged'
        elif doc.i == 2:
            assert doc.get_extra_fields()['j'] == 'test2'
        else:
            assert False


def test_update_list():
    s = Session.connect('unit-testing')
    s.clear_collection(TIntListDoc)

    tIntList = TIntListDoc(intlist=[1,2])
    s.insert(tIntList)

    # pull out of db
    tFetched = s.query(TIntListDoc).one()

    assert sorted([1,2]) == sorted(tFetched.intlist)

    # append to list, update
    l = tFetched.intlist
    l.append(3)
    s.update(tFetched)

    # pull out of db
    tFetched = s.query(TIntListDoc).one()

    assert sorted([1,2,3]) == sorted(tFetched.intlist)

    tFetched.intlist.remove(1)
    s.update(tFetched)

    tFetched = s.query(TIntListDoc).one()

    assert sorted([2,3]) == sorted(tFetched.intlist)
