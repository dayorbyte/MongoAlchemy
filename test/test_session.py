from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField
from mongoalchemy.fields import *
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


def test_session():
    s = Session.connect('unit-testing')
    s.clear_collection(T)
    s.insert(T(i=1))
    s.clear()
    s.end()

def test_context_manater():
    with Session.connect('unit-testing') as s:
        s.clear_collection(T)
        t = T(i=5)

def test_safe():
    s = Session.connect('unit-testing', safe=True)
    assert s.safe == True
    s = Session.connect('unit-testing', safe=False)
    assert s.safe == False

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
