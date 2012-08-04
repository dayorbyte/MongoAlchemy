from mongoalchemy.util import classproperty, UNSET

def test_class_properties():
    class A(object):
        a = 1
        b = 2
        @classproperty
        def c(cls):
            return cls.a+cls.b
    assert A.c == 3

def test_UNSET():
    # for coverage
    r = repr(UNSET)
    assert UNSET == UNSET
    assert UNSET is not None