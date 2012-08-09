from nose.tools import *
from mongoalchemy.fields import *
from test.util import known_failure

# List Field
@raises(BadValueException)
def list_wrong_type_test():
    ListField(IntField()).wrap(4)

@raises(BadValueException)
def list_bad_type_test_unwrap():
    ListField(IntField()).unwrap(4)

@raises(BadValueException)
def list_wrong_child_type_test():
    ListField(StringField()).wrap([4])

@raises(Exception)
def list_bad_child_type_test_wrap():
    ListField(int).wrap([4])


@raises(Exception)
def list_bad_child_test_unwrap():
    ListField(IntField()).unwrap(['4'])

@raises(BadValueException)
def list_too_long_test():
    ListField(StringField(), max_capacity=4).unwrap([x for x in '12345'])

@raises(BadValueException)
def list_too_short_test():
    ListField(StringField(), min_capacity=4).wrap([x for x in '123'])

def list_just_right_test():
    ListField(StringField(), min_capacity=3, max_capacity=3).wrap([x for x in '123'])

def list_value_test():
    s = ListField(StringField())
    foo = [x for x in '12345']
    assert s.unwrap(s.wrap(foo)) == foo

def list_default_test():
    s = ListField(StringField(), default_empty=True)
    assert s.default == []

# Set Field
@raises(BadValueException)
def set_wrong_type_test_wrap():
    SetField(IntField()).wrap([4])

@raises(BadValueException)
def set_wrong_type_test_unwrap():
    SetField(IntField()).unwrap((4,))


@raises(BadValueException)
def set_wrong_child_type_test():
    SetField(StringField()).wrap(set([4]))

def set_no_rel_test():
    SetField(StringField()).rel()


@raises(Exception)
def set_bad_child_type_test():
    SetField(int).wrap(set([4]))

@raises(BadValueException)
def set_too_long_test():
    SetField(StringField(), max_capacity=4).wrap(set([x for x in '12345']))

@raises(BadValueException)
def set_too_short_test():
    SetField(StringField(), min_capacity=4).wrap(set([x for x in '123']))

def set_just_right_test():
    SetField(StringField(), min_capacity=3, max_capacity=3).wrap(set([x for x in '123']))

def set_value_test():
    s = SetField(StringField())
    foo = set([x for x in '12345'])
    assert s.unwrap(s.wrap(foo)) == foo

def set_default_test():
    assert SetField(StringField(), default_empty=True).default == set()
    assert SetField(StringField(), default=set([3])).default == set([3])
