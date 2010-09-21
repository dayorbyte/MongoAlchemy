
from mongomapper.session import Session
from mongomapper.document import Document, Index
from mongomapper.fields import *

def main():
    s = Session.connect('mongomapper')
    
    class User(Document):
        
        name_index = Index().ascending('name').unique_index()
        
        name = StringField()
        email = StringField()
        def __str__(self):
            return '%s (%s)' % (self.name, self.email)
    
    s.clear_collection(User)
    
    
    u = User(name='jeff', email='jeff@qcircles.net')
    
    s.insert(u)
    
    def print_all():
        for u in s.query(User).filter(User.f.name > 'ivan', User.f.name < 'katie' ):
            print u
    
    print_all()
    
    update = s.query(User).filter(User.f.name > 'ivan', User.f.name < 'katie' ).set(User.f.email, 'jeff2@qcircles.net')
    update.execute()
    print_all()


if __name__ == '__main__':
    main()
