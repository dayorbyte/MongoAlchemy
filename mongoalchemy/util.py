
def classproperty(fun):
    class Descriptor(property):
        def __get__(self, instance, owner):
            return fun(owner)
    return Descriptor()