from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, DocumentField
from mongoalchemy.fields import *
from test.util import known_failure

class T(Document):
    i = IntField()

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
