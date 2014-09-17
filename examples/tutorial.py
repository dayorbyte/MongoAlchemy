
# Create a mapping class
from mongoalchemy.document import Document
from mongoalchemy.fields import *

class BloodDonor(Document):
    first_name = StringField()
    last_name = StringField()
    age = IntField(min_value=0)

    gender = EnumField(StringField(), 'male', 'female')
    blood_type = EnumField(StringField(), 'O+','A+','B+','AB+','O-','A-','B-','AB-')
    def __str__(self):
        return '%s %s (%s; Age: %d; Type: %s)' % (self.first_name, self.last_name,
            self.gender, self.age, self.blood_type)


# Create A session, insert an object
from mongoalchemy.session import Session
session = Session.connect('mongoalchemy-tutorial')
session.clear_collection(BloodDonor)

donor = BloodDonor(first_name='Jeff', last_name='Jenkins',
            age=28, blood_type='O+', gender='male')
session.save(donor)

# Add some more objects for the querying section

session.save(BloodDonor(first_name='Jeff', last_name='Winger', age=38, blood_type='O+', gender='male'))
session.save(BloodDonor(first_name='Britta', last_name='Perry', age=27, blood_type='A+', gender='female'))
session.save(BloodDonor(first_name='Abed', last_name='Nadir', age=29, blood_type='O+', gender='male'))
session.save(BloodDonor(first_name='Shirley', last_name='Bennett', age=39, blood_type='O-', gender='female'))

# Querying

for donor in session.query(BloodDonor).filter(BloodDonor.first_name == 'Jeff'):
    print donor

for donor in session.query(BloodDonor).filter(
    BloodDonor.first_name == 'Jeff',
    BloodDonor.age < 30):
   print donor

for donor in session.query(BloodDonor).filter(
    BloodDonor.first_name == 'Jeff').filter(
    BloodDonor.age < 30):
   print donor

query = session.query(BloodDonor).filter(BloodDonor.first_name == 'Jeff', BloodDonor.last_name == 'Jenkins')
query.inc(BloodDonor.age, 1).set(BloodDonor.blood_type, 'O-').execute()
print query.one()


