# The MIT License
#
# Copyright (c) 2010 Jeffrey Jenkins
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from pprint import pprint
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index
from mongoalchemy.fields import *

def main():
    class Address(Document):
        street_address = StringField()
        city = StringField()
        state_province = StringField()
        country = StringField()

    class User(Document):

        name_index = Index().ascending('name').unique()

        name = StringField()
        email = StringField()

        address = DocumentField(Address)

        def __str__(self):
            return '%s (%s)' % (self.name, self.email)


    with Session.connect('mongoalchemy') as s:
        def print_all():
            for u in s.query(User).filter(User.address.country == 'USA' ):
                print u

        s.clear_collection(User)

        a = Address(street_address='123 4th ave', city='NY', state_province='NY', country='USA')
        u = User(name='jeff', email='jeff@qcircles.net', address=a)
        s.save(u)
        print u.mongo_id

        query = User.address.country == 'USA'

        print_all()

        update = s.query(User).filter(User.name > 'ivan', User.name < 'katie' ).set(User.email, 'jeff2@qcircles.net')
        update.execute()
        print_all()


if __name__ == '__main__':
    main()
