from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField
from mongoalchemy.fields import *
from test.util import known_failure

class T(Document):
    i = IntField()
    l = ListField(IntField(), required=False, on_update='$pushAll')

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
    
