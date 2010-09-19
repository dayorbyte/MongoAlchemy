
from mongomapper.session import Session
from mongomapper.document import Document, Index
from mongomapper.fields import *

def main():
    s = Session.connect('mongomapper')
    
    class User(Document):
        
        name_index = Index().ascending('name').unique()
        
        name = StringField()
        email = StringField()
        def __str__(self):
            return '%s (%s)' % (self.name, self.email)
    
    u = User(name='jeff', email='jeff@qcircles.net')
    
    
    s.execute(u)
    
    for u in s.query(User):
        print u



if __name__ == '__main__':
    # try:
    main()
    # except Exception, e:
    #     raise e.exception