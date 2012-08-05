'''
This page is going to go through some more advanced modeling techniques 
using forward and self-references

'''


from mongoalchemy.document import Document
from mongoalchemy.fields import *
from datetime import datetime
from pprint import pprint
class Event(Document):
    name = StringField()
    children = ListField(DocumentField('Event'))
    begin = DateTimeField()
    end = DateTimeField()
    
    def __init__(self, name, parent=None):
        Document.__init__(self, name=name)
        self.children = []
        if parent is not None:
            parent.children.append(self)
    def __enter__(self):
        self.begin = datetime.utcnow()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = datetime.utcnow()

with Event('request') as root:
    with Event('main_func', root) as br:
        with Event('setup', br):
            pass
        with Event('handle', br):
            pass
        with Event('teardown', br):
            pass
    with Event('cleanup', root):
        pass

pprint(root.wrap())
