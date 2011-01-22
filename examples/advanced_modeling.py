from mongoalchemy.document import Document, DocumentField
from mongoalchemy.fields import *
from datetime import datetime

class Event(Document):
    name = StringField()
    children = ListField(DocumentField('Event'), default=[])
    start = DateTimeField()
    end = DateTimeField()
    
    def __init__(self, name, parent=None):
        Document.__init__(self, name=name)
        if parent != None:
            parent.children.append(self)
    def __enter__(self):
        self.start = datetime.utcnow()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = datetime.utcnow()

root = Event('request')
with Event('main_func', root) as br:
    with Event('setup', br):
        pass
    with Event('handle', br):
        pass
    with Event('teardown', br):
        pass
with Event('cleanup', root):
    pass

print root.wrap()
