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

from mongoalchemy.fields.base import *


class SRefField(Field):
    ''' A Simple RefField (SRefField) looks like an ObjectIdField in the 
        database, but acts like a mongo DBRef.  It uses the passed in type to 
        determine where to look for the object (and assumes the current 
        database).
    '''
    has_subfields = True
    has_autoload = True
    def __init__(self, type, **kwargs):
        from mongoalchemy.fields import DocumentField

        super(SRefField, self).__init__(**kwargs)
        
        self.type = type
        if not isinstance(type, DocumentField):
            self.type = DocumentField(type)
    def wrap(self, value):
        self.validate_wrap(value)
        return value
    def unwrap(self, value, fields=None, session=None):
        self.validate_unwrap(value)
        return value
    def validate_wrap(self, value):
        if not isinstance(value, ObjectId):
            self._fail_validation_type(value, ObjectId)
    validate_unwrap = validate_wrap
        

class RefField(Field):
    ''' A ref field wraps a mongo DBReference.  It DOES NOT currently handle 
        saving the referenced object or updates to it, but it can handle 
        auto-loading.
    '''
    #: If this kind of field can have sub-fields, this attribute should be True
    has_subfields = True
    has_autoload = True

    def __init__(self, type=None, db=None, namespace='global', **kwargs):
        ''' :param type: (optional) the Field type to use for the values.  It 
                must be a DocumentField.  If you want to save refs to raw mongo 
                objects, you can leave this field out
            :param db: (optional) The database to load the object from.  
                Defaults to the same database as the object this field is 
                bound to.
            :param namespace: If using the namespace system and using a 
                collection name instead of a type, selects which namespace to 
                use
        '''
        from mongoalchemy.fields import DocumentField
        if type and not isinstance(type, DocumentField):
            type = DocumentField(type)

        super(RefField, self).__init__(**kwargs)
        self.type = type
        self.namespace = namespace
        self.db = db
    
    def wrap(self, value):
        ''' Validate ``value`` and then use the value_type to wrap the 
            value'''

        self.validate_wrap(value)
        value.type = self.type
        return value

            
    def unwrap(self, value, fields=None, session=None):
        ''' If ``autoload`` is False, return a DBRef object.  Otherwise load
            the object.  
        '''
        self.validate_unwrap(value)
        value.type = self.type
        return value
    
    def validate_unwrap(self, value, session=None):
        ''' Validates that the DBRef is valid as well as can be done without
            retrieving it.
        '''
        if not isinstance(value, DBRef):
            self._fail_validation_type(value, DBRef)
        if self.type:
            expected = self.type.type.get_collection_name()
            got = value.collection
            if expected != got:
                self._fail_validation(value, '''Wrong collection for reference: '''
                                      '''got "%s" instead of "%s" ''' % (got, expected))
        if self.db and self.db != value.database:
            self._fail_validation(value, '''Wrong database for reference: '''
                                  ''' got "%s" instead of "%s" ''' % (value.database, self.db) ) 
    validate_wrap = validate_unwrap
