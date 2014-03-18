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

from __future__ import print_function
from mongoalchemy.py3compat import *

from mongoalchemy.fields.base import *
from bson import DBRef

class RefBase(Field):
    def rel(self, allow_none=False):
        """ Used to create an attribute which will auto-dereference
            a RefField or SRefField.

            **Example**::

                employer_ref = SRefField(Employer)
                employer = employer_ref.rel()

        """
        return Proxy(self, allow_none=allow_none)

class SRefField(RefBase):
    ''' A Simple RefField (SRefField) looks like an ObjectIdField in the
        database, but acts like a mongo DBRef.  It uses the passed in type to
        determine where to look for the object (and assumes the current
        database).
    '''
    has_subfields = True
    has_autoload = True
    def __init__(self, type, db=None, **kwargs):
        from mongoalchemy.fields import DocumentField

        super(SRefField, self).__init__(**kwargs)

        self.type = type
        if not isinstance(type, DocumentField):
            self.type = DocumentField(type)
        self.db = db
    def schema_json(self):
        super_schema = super(SRefField, self).schema_json()
        return dict(subtype=self.type.schema_json(),
                    db=self.db, **super_schema)
    def _to_ref(self, doc):
        return doc.mongo_id
    def dereference(self, session, ref, allow_none=False):
        """ Dereference an ObjectID to this field's underlying type """
        ref = DBRef(id=ref, collection=self.type.type.get_collection_name(),
                    database=self.db)
        ref.type = self.type.type
        return session.dereference(ref, allow_none=allow_none)
    def set_parent_on_subtypes(self, parent):
        self.type.parent = parent
    def wrap(self, value):
        self.validate_wrap(value)
        return value
    def unwrap(self, value, fields=None, session=None):
        self.validate_unwrap(value)
        return value
    def validate_unwrap(self, value, session=None):
        if not isinstance(value, ObjectId):
            self._fail_validation_type(value, ObjectId)
    validate_wrap = validate_unwrap


class RefField(RefBase):
    ''' A ref field wraps a mongo DBReference.  It DOES NOT currently handle
        saving the referenced object or updates to it, but it can handle
        auto-loading.
    '''
    #: If this kind of field can have sub-fields, this attribute should be True
    has_subfields = True
    has_autoload = True

    def __init__(self, type=None, db=None, db_required=False, namespace='global', **kwargs):
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
        self.db_required = db_required
        self.type = type
        self.namespace = namespace
        self.db = db
        self.parent = None

    def schema_json(self):
        super_schema = super(RefField, self).schema_json()
        subtype = self.type
        if subtype is not None:
            subtype = subtype.schema_json()
        return dict(db_required=self.db_required,
                    subtype=subtype,
                    namespace=self.namespace,
                    db=self.db, **super_schema)

    def wrap(self, value):
        ''' Validate ``value`` and then use the value_type to wrap the
            value'''

        self.validate_wrap(value)
        value.type = self.type
        return value

    def _to_ref(self, doc):
        return doc.to_ref(db=self.db)

    def unwrap(self, value, fields=None, session=None):
        ''' If ``autoload`` is False, return a DBRef object.  Otherwise load
            the object.
        '''
        self.validate_unwrap(value)
        value.type = self.type
        return value

    def dereference(self, session, ref, allow_none=False):
        """ Dereference a pymongo "DBRef" to this field's underlying type """
        from mongoalchemy.document import collection_registry
        # TODO: namespace support
        ref.type = collection_registry['global'][ref.collection]
        obj = session.dereference(ref, allow_none=allow_none)
        return obj
    def set_parent_on_subtypes(self, parent):
        if self.type:
            self.type.parent = parent

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
        if self.db_required and not value.database:
            self._fail_validation(value, 'db_required=True, but not database specified')
        if self.db and value.database and self.db != value.database:
            self._fail_validation(value, '''Wrong database for reference: '''
                                  ''' got "%s" instead of "%s" ''' % (value.database, self.db) )
    validate_wrap = validate_unwrap

class Proxy(object):
    def __init__(self, field, allow_none=False):
        self.allow_none = allow_none
        self.field = field
    def __get__(self, instance, owner):
        if instance is None:
            return self.field
        session = instance._get_session()
        ref = getattr(instance, self.field._name)
        if ref is None:
            return None
        return self.field.dereference(session, ref, allow_none=self.allow_none)
    def __set__(self, instance, value):
        assert instance is not None
        setattr(instance, self.field._name, self.field._to_ref(value))


