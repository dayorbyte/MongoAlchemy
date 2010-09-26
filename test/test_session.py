from nose.tools import *
from pprint import pprint
from mongoalchemy.session import Session, FailedOperation
from mongoalchemy.document import Document, Index, DocumentField
from mongoalchemy.fields import *
from test.util import known_failure

class T(Document):
    i = IntField()

def test_session():
    s = Session.connect('unit-testing')
    s.clear_collection(T)
    s.execute(T(i=1))
    s.clear()
    s.end()

@raises(FailedOperation)
def test_failed_op():
    s = Session.connect('unit-testing')
    s.clear_collection(T)
    t = T(i=5)
    try:
        raise Exception()
    except Exception, e:
        fo = FailedOperation(t, e)
        print fo
        raise fo