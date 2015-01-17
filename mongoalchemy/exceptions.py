from __future__ import print_function
from mongoalchemy.py3compat import *

class BadResultException(Exception):
    ''' Only raised right now when .one() finds more than one object '''
    pass

class BadValueException(Exception):
    ''' An exception which is raised when there is something wrong with a
        value'''
    def __init__(self, name, value, reason, cause=None):
        self.name = name
        self.value = value
        self.cause = cause
        message = 'Bad value for field of type "%s".  Reason: "%s".' % (name, reason)
        if cause is not None:
            message = '%s Cause: %s' % (message, cause)
        Exception.__init__(self, message)

class BadReferenceException(Exception):
    pass

class InvalidConfigException(Exception):
    ''' Raised when a bad value is passed in for a configuration that expects
        its values to obey certain constraints.'''
    pass

class DocumentException(Exception):
    ''' Base for all document-related exceptions'''
    pass

class MissingValueException(DocumentException):
    ''' Raised when a required field isn't set '''
    pass

class ExtraValueException(DocumentException):
    ''' Raised when a value is passed in with no corresponding field '''
    pass

class FieldNotRetrieved(DocumentException):
    ''' If a partial document is loaded from the database and a field which
        wasn't retrieved is accessed this exception is raised'''
    pass


class BadFieldSpecification(Exception):
    ''' An exception that is raised when there is an error in creating a
        field'''
    pass


class TransactionException(Exception):
    """ Exception which occurs when an invalid operation is called during a
        transaction """

class SessionCacheException(Exception):
    """ Exception when an error has occured with the MA caching mechanism """

class InvalidUpdateException(Exception):
    ''' Exception when an Update op is malformed '''
