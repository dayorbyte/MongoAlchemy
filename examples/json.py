from mongoalchemy.document import Document
from mongoalchemy.fields import StringField

class A(Document):
    b = StringField()
    c = StringField()

a = A.unwrap({'b' : 'some val 1', 'c' : 'some val 2'})
print a.b, a.c