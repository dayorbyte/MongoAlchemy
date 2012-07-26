from nose.tools import *
from mongoalchemy.fields import *
from test.util import known_failure
from datetime import datetime
from bson.binary import Binary

# Field Tests

@raises(NotImplementedError)
def test_unimplemented_wrap():
    Field().wrap({})

@raises(NotImplementedError)
def test_unimplemented_unwrap():
    Field().unwrap({})

@raises(NotImplementedError)
def test_unimplemented_is_valid_wrap():
    Field().is_valid_wrap({})

@raises(NotImplementedError)
def test_unimplemented_is_valid_unwrap():
    Field().is_valid_unwrap({})

def test_is_valid_unwrap():
    assert IntField().is_valid_unwrap('') == False
    assert IntField().is_valid_unwrap(1) == True

def test_allow_none():
    assert IntField(allow_none=True).is_valid_unwrap(None) == True
    assert IntField(allow_none=True).is_valid_wrap(None) == True
    assert IntField().is_valid_unwrap(None) == False
    assert IntField().is_valid_wrap(None) == False
    assert IntField(allow_none=True).wrap(None) == None
    assert IntField(allow_none=True).unwrap(None) == None
    assert IntField(default=None).unwrap(None) == None

def test_id_attr():
    assert IntField().is_id == False
    assert IntField(db_field='_id').is_id == True
    assert IntField(_id=True).is_id == True

@raises(InvalidConfigException)
def test_bad_id_attr():
    IntField(db_field='foo', _id=True)

@raises(InvalidConfigException)
def test_bad_on_update_value():
    IntField(on_update='set')

@raises(BadValueException)
def test_validate_unwrap_fail():
    StringField().unwrap(4)

def test_custom_validator():
    field = IntField(validator=lambda x : x == 0)
    assert field.is_valid_wrap(0) == True
    assert field.is_valid_wrap(2) == False
    assert field.is_valid_unwrap(0) == True
    assert field.is_valid_unwrap(2) == False

    field = IntField(wrap_validator=lambda x : x == 0)
    assert field.is_valid_wrap(0) == True
    assert field.is_valid_wrap(2) == False
    
    field = IntField(unwrap_validator=lambda x : x == 0)
    assert field.is_valid_unwrap(0) == True
    assert field.is_valid_unwrap(2) == False

# String Tests
@raises(BadValueException)
def string_wrong_type_test():
    StringField().wrap(4)

@raises(BadValueException)
def string_too_long_test():
    StringField(max_length=4).wrap('12345')

@raises(BadValueException)
def string_too_short_test():
    StringField(min_length=4).wrap('123')

def string_value_test():
    s = StringField()
    assert s.wrap('foo') == 'foo'
    assert s.unwrap('bar') == 'bar'

# Bool Field
@raises(BadValueException)
def bool_wrong_type_test():
    BoolField().wrap(4)

def bool_value_test():
    b = BoolField()
    assert b.wrap(True) == True
    assert b.unwrap(False) == False

# Number Fields
@raises(BadValueException)
def int_wrong_type_test():
    IntField().wrap('4')

@raises(BadValueException)
def int_too_high_test():
    IntField(max_value=4).wrap(5)

@raises(BadValueException)
def int_too_low_test():
    IntField(min_value=4).wrap(3)

def int_value_test():
    s = IntField()
    assert s.wrap(1) == 1
    assert s.unwrap(1564684654) == 1564684654

@raises(BadValueException)
def float_wrong_type_test():
    FloatField().wrap('1')

# Date/time field
@raises(BadValueException)
def datetime_wrong_type_test():
    DateTimeField().wrap(4)

@raises(BadValueException)
def datetime_too_new_test():
    DateTimeField(max_date=datetime(2009, 7, 9)).wrap(datetime(2009, 7, 10))

@raises(BadValueException)
def datetime_too_old_test():
    DateTimeField(min_date=datetime(2009, 7, 9)).wrap(datetime(2009, 7, 8))

def datetime_value_test():
    s = DateTimeField()
    assert s.wrap(datetime(2009, 7, 9)) == datetime(2009, 7, 9)
    assert s.unwrap(datetime(2009, 7, 9)) == datetime(2009, 7, 9), (s.unwrap(datetime(2009, 7, 9)),)

@raises(BadValueException)
def test_tz_unaware():
    s = DateTimeField(use_tz=True)
    s.wrap(datetime.now())

def test_tz_aware():
    import pytz
    from mongoalchemy.session import Session
    from mongoalchemy.document import Document
    # doc
    class DocTZ(Document):
        time = DateTimeField(use_tz=True)
    class DocNoTZ(Document):
        time = DateTimeField(use_tz=False)

    # timezone -- choose one where the author doesn't live
    eastern = pytz.timezone('Australia/Melbourne')
    utc = pytz.utc
    # session
    s = Session.connect('unit-testing', timezone=eastern)
    s.clear_collection(DocTZ)
    s.clear_collection(DocNoTZ)
    # time
    local = eastern.localize(datetime.now())
    local = local.replace(microsecond=0)
    doc = DocTZ(time=local)
    s.insert(doc)

    doc = s.query(DocTZ).one()
    assert doc.time == local, (doc.time, local)

    # do the no timezone case for code coverage
    s.insert(DocNoTZ(time=datetime(2012, 1, 1)))
    obj = s.query(DocNoTZ).one()




# Anything Field
def test_anything():
    a = AnythingField()
    foo = {'23423423' : [23423432], 'fvfvf' : { 'a' : [] }}
    assert a.is_valid_wrap(foo)
    assert a.is_valid_unwrap(foo)
    assert a.unwrap(a.wrap(foo)) == foo

#ObjectID Field
@raises(BadValueException)
def objectid_wrong_type_test():
    from bson.objectid import ObjectId
    ObjectIdField().wrap(1)

@raises(BadValueException)
def objectid_wrong_type_unwrap_test():
    from bson.objectid import ObjectId
    ObjectIdField().unwrap(1)

def test_object_id_auto():
    from mongoalchemy.document import Document
    class A(Document):
        idf = ObjectIdField(auto=True, required=False)
    assert 'idf' in A().wrap()

#ObjectID Field
@raises(BadValueException)
def objectid_wrong_hex_length_test():
    from bson.objectid import ObjectId
    ObjectIdField().wrap('c9e2587eae7dd6064000000')

def objectid_value_test():
    from bson.objectid import ObjectId
    o = ObjectIdField()
    oid = ObjectId('4c9e2587eae7dd6064000000')
    assert o.unwrap(o.wrap(oid)) == oid
    
    oid2 = '4c9e2587eae7dd6064000000'
    assert o.unwrap(o.wrap(oid2)) == oid

    assert o.wrap(o.unwrap(oid.binary)) == oid

    assert isinstance(o.gen(), ObjectId)


# TupleField
@raises(BadValueException)
def tuple_wrong_type_test_wrap():
    TupleField(IntField()).wrap(4)

@raises(BadValueException)
def tuple_wrong_type_test_unwrap():
    TupleField(IntField()).unwrap(4)

@raises(BadValueException)
def first_type_wrong_test():
    TupleField(IntField(), IntField(), IntField()).wrap(('1', 2, 3))

@raises(BadValueException)
def third_type_wrong_test():
    TupleField(IntField(), IntField(), IntField()).wrap((1, 2, '3'))

@raises(BadValueException)
def first_type_wrong_test_unwrap():
    TupleField(IntField(), IntField(), IntField()).unwrap(['1', 2, 3])

@raises(BadValueException)
def third_type_wrong_test_unwrap():
    TupleField(IntField(), IntField(), IntField()).unwrap([1, 2, '3'])

def tuple_value_test():
    s = TupleField(IntField(), StringField(), ListField(IntField()))
    before = (1, '2', [3,3,3])
    after = [1, '2', [3,3,3]]
    assert s.wrap(before) == after, s.wrap(before)
    assert s.unwrap(after) == before, s.unwrap(after)

# EnumField
@raises(BadValueException)
def enum_wrong_type_test_wrap():
    EnumField(StringField(), '1', '2', '3', '4').wrap(4)

@raises(BadValueException)
def enum_wrong_type_test_unwrap():
    EnumField(StringField(), '1', '2', '3', '4').unwrap(4)

@raises(BadValueException)
def enum_wrong_value_test_wrap():
    EnumField(IntField(), 1, 3).wrap(2)

@raises(BadValueException)
def enum_wrong_value_test_unwrap():
    EnumField(IntField(), 1, 3).unwrap(2)

def enum_value_test():
    s = EnumField(ListField(IntField()), [1,2], [3,4])
    assert s.wrap([1,2]) == [1,2]
    assert s.unwrap([3,4]) == [3,4]


# Binary Field

@raises(BadValueException)
def binary_wrong_type_test_wrap():
    BinaryField().wrap(4)

@raises(BadValueException)
def binary_wrong_type_test_unwrap():
    BinaryField().unwrap(4)

def binary_value_test():
    s = BinaryField()
    assert s.wrap(Binary(bytes('foo'.encode('ascii')))) == Binary(bytes('foo'.encode('ascii')))
    assert s.wrap(bytes('foo'.encode('ascii'))) == Binary(bytes('foo'.encode('ascii')))
    assert s.unwrap(Binary(bytes('foo'.encode('ascii')))) == Binary(bytes('foo'.encode('ascii')))
