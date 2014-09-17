from mongoalchemy.document import Document
from mongoalchemy.fields import *
from datetime import datetime
from pprint import pprint

class HashField(StringField):
    def set_value(self, instance, value, from_db=False):
        if from_db:
            super(HashField, self).set_value(instance, value)
        else:
            super(HashField, self).set_value(instance, str(hash(value)))

class User(Document):
    password = HashField()

from mongoalchemy.session import Session
session = Session.connect('mongoalchemy-tutorial')
session.clear_collection(User)

user = User(password='pw')
print user.password
user.password = 'newpw'
print user.password

session.save(user)

loaded_user = session.query(User).one()

print loaded_user.password
print loaded_user.password