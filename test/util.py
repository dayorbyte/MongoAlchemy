from __future__ import print_function
from mongoalchemy.py3compat import *

from functools import wraps

def known_failure(fun):
    @wraps(fun)
    def wrapper(*args, **kwds):
        try:
            fun(*args, **kwds)
            raise Exception('Known failure passed! %s' % fun.__name__)
        except:
            pass
    return wrapper
