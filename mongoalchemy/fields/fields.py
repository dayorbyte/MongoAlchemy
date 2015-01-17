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


class PrimitiveField(Field):
    ''' Primitive fields are fields where a single constructor can be used
        for wrapping and unwrapping an object.'''

    valid_modifiers = SCALAR_MODIFIERS

    def __init__(self, constructor, **kwargs):
        super(PrimitiveField, self).__init__(**kwargs)
        self.constructor = constructor

    def wrap(self, value):
        self.validate_wrap(value)
        return self.constructor(value)
    def unwrap(self, value, session=None):
        self.validate_unwrap(value)
        return self.constructor(value)

class StringField(PrimitiveField):
    ''' Unicode Strings.  ``unicode`` is used to wrap and unwrap values,
        and any subclass of basestring is an acceptable input'''
    def __init__(self, max_length=None, min_length=None, **kwargs):
        ''' :param max_length: maximum string length
            :param min_length: minimum string length
            :param kwargs: arguments for :class:`Field`
        '''
        self.max = max_length
        self.min = min_length
        super(StringField, self).__init__(constructor=unicode, **kwargs)

    def validate_wrap(self, value):
        ''' Validates the type and length of ``value`` '''
        if not isinstance(value, basestring):
            self._fail_validation_type(value, basestring)
        if self.max is not None and len(value) > self.max:
            self._fail_validation(value, 'Value too long (%d)' % len(value))
        if self.min is not None and len(value) < self.min:
            self._fail_validation(value, 'Value too short (%d)' % len(value))

class RegExStringField(PrimitiveField):
    ''' Unicode Strings.  ``unicode`` is used to wrap and unwrap values,
        and any subclass of basestring is an acceptable input, as long as
        it matches the provided regex.'''
    def __init__(self, regex, **kwargs):
        ''' :param regex: instance of :class: `RegexObject` to match against
            :param kwargs: arguments for :class:`Field`
        '''
        self.regex = regex
        super(RegExStringField, self).__init__(constructor=unicode, **kwargs)

    def validate_wrap(self, value):
        ''' Validates the type and length of ``value`` '''
        if not isinstance(value, basestring):
            self._fail_validation_type(value, basestring)
        if self.regex.match(value) is None:
            self._fail_validation(value, 'Value does not match regular expression')

class BinaryField(PrimitiveField):
    def __init__(self, **kwargs):
        super(BinaryField, self).__init__(constructor=Binary, **kwargs)

    def validate_wrap(self, value):
        if not isinstance(value, bytes) and not isinstance(value, Binary):
            self._fail_validation_type(value, str, Binary)

class BoolField(PrimitiveField):
    ''' ``True`` or ``False``.'''
    def __init__(self, **kwargs):
        super(BoolField, self).__init__(constructor=bool, **kwargs)
    def validate_wrap(self, value):
        if not isinstance(value, bool):
            self._fail_validation_type(value, bool)

class NumberField(PrimitiveField):
    ''' Base class for numeric fields '''

    valid_modifiers = NUMBER_MODIFIERS

    def __init__(self, constructor, min_value=None, max_value=None, **kwargs):
        ''' :param max_value: maximum value
            :param min_value: minimum value
            :param kwargs: arguments for :class:`Field`
        '''
        super(NumberField, self).__init__(constructor=constructor, **kwargs)
        self.min = min_value
        self.max = max_value

    def schema_json(self):
        super_schema = super(NumberField, self).schema_json()
        return dict(min_value=self.min,
                    max_value=self.max, **super_schema)

    def validate_wrap(self, value, *types):
        ''' Validates the type and value of ``value`` '''
        for type in types:
            if isinstance(value, type):
                break
        else:
            self._fail_validation_type(value, *types)

        if self.min is not None and value < self.min:
            self._fail_validation(value, 'Value too small')
        if self.max is not None and value > self.max:
            self._fail_validation(value, 'Value too large')

class IntField(NumberField):
    ''' Subclass of :class:`~NumberField` for ``int``'''
    def __init__(self, **kwargs):
        ''' :param max_length: maximum value
            :param min_length: minimum value
            :param kwargs: arguments for :class:`Field`
        '''
        super(IntField, self).__init__(constructor=int, **kwargs)
    def validate_wrap(self, value):
        ''' Validates the type and value of ``value`` '''
        NumberField.validate_wrap(self, value, int, long)

class FloatField(NumberField):
    ''' Subclass of :class:`~NumberField` for ``float`` '''
    def __init__(self, **kwargs):
        ''' :param max_value: maximum value
            :param min_value: minimum value
            :param kwargs: arguments for :class:`Field`
        '''
        super(FloatField, self).__init__(constructor=float, **kwargs)
    def validate_wrap(self, value):
        ''' Validates the type and value of ``value`` '''
        return NumberField.validate_wrap(self, value, float, int)

class DateTimeField(PrimitiveField):
    ''' Field for datetime objects. '''

    has_autoload = True

    def __init__(self, min_date=None, max_date=None, use_tz=False, **kwargs):
        ''' :param max_date: maximum date
            :param min_date: minimum date
            :param use_tz: Require a timezone-aware datetime (via pytz).
                Values are converted to UTC before saving.  min and max dates
                are currently ignored when use_tz is on.  You MUST pass a
                timezone into the session
            :param kwargs: arguments for :class:`Field`
        '''
        super(DateTimeField, self).__init__(lambda dt : dt, **kwargs)
        self.min = min_date
        self.max = max_date
        self.use_tz = use_tz
        if self.use_tz:
            import pytz
            self.utc = pytz.utc
            assert self.min is None and self.max is None
    def schema_json(self):
        super_schema = super(DateTimeField, self).schema_json()
        return dict(min_date=self.min,
                    max_date=self.max,
                    use_tz=self.use_tz, **super_schema)

    def wrap(self, value):
        self.validate_wrap(value)
        value = self.constructor(value)
        if self.use_tz:
            return value
        return value
    def unwrap(self, value, session=None):
        self.validate_unwrap(value)
        value = self.constructor(value)
        if value.tzinfo is not None:
            import pytz
            value = value.replace(tzinfo=pytz.utc)
            if session and session.timezone:
                value = value.astimezone(session.timezone)
        return value

    def localize(self, session, value):
        if not self.use_tz:
            return value
        return value.astimezone(session.timezone)

    def validate_wrap(self, value):
        ''' Validates the value's type as well as it being in the valid
            date range'''
        if not isinstance(value, datetime):
            self._fail_validation_type(value, datetime)

        if self.use_tz and value.tzinfo is None:
            self._fail_validation(value, '''datetime is not timezone aware and use_tz is on.  make sure timezone is set on the session''')

        # if using timezone support it isn't clear how min and max should work,
        # so the problem is being punted on for now.
        if self.use_tz:
            return

        # min/max
        if self.min is not None and value < self.min:
            self._fail_validation(value, 'DateTime too old')
        if self.max is not None and value > self.max:
            self._fail_validation(value, 'DateTime too new')

class TupleField(Field):
    ''' Represents a field which is a tuple of a fixed size with specific
        types for each element in the field.

        **Examples** ``TupleField(IntField(), BoolField())`` would accept
        ``[19, False]`` as a value for both wrapping and unwrapping. '''

    # uses scalar modifiers since it is not variable length
    valid_modifiers = SCALAR_MODIFIERS

    def __init__(self, *item_types, **kwargs):
        ''' :param item_types: instances of :class:`Field`, in the order they \
                    will appear in the tuples.
            :param kwargs: arguments for :class:`Field`
        '''
        super(TupleField, self).__init__(**kwargs)
        self.size = len(item_types)
        self.types = item_types

    def schema_json(self):
        super_schema = super(TupleField, self).schema_json()
        types = [t.schema_json() for t in self.types]
        return dict(types=types, **super_schema)

    def set_parent_on_subtypes(self, parent):
        for type in self.types:
            type._set_parent(parent)

    def validate_wrap(self, value):
        ''' Checks that the correct number of elements are in ``value`` and that
            each element validates agains the associated Field class
        '''
        if not isinstance(value, list) and not isinstance(value, tuple):
            self._fail_validation_type(value, tuple, list)

        for field, value in izip(self.types, list(value)):
            field.validate_wrap(value)

    def validate_unwrap(self, value):
        ''' Checks that the correct number of elements are in ``value`` and that
            each element validates agains the associated Field class
        '''
        if not isinstance(value, list) and not isinstance(value, tuple):
            self._fail_validation_type(value, tuple, list)

        for field, value in izip(self.types, value):
            field.validate_unwrap(value)

    def wrap(self, value):
        ''' Validate and then wrap ``value`` for insertion.

            :param value: the tuple (or list) to wrap
        '''
        self.validate_wrap(value)
        ret = []
        for field, value in izip(self.types, value):
            ret.append(field.wrap(value))
        return ret

    def unwrap(self, value, session=None):
        ''' Validate and then unwrap ``value`` for object creation.

            :param value: list returned from the database.
        '''
        self.validate_unwrap(value)
        ret = []
        for field, value in izip(self.types, value):
            ret.append(field.unwrap(value, session=session))
        return tuple(ret)

class GeoField(TupleField):
    def __init__(self, **kwargs):
        ''' :param item_types: instances of :class:`Field`, in the order they \
                    will appear in the tuples.
            :param kwargs: arguments for :class:`Field`
        '''
        super(GeoField, self).__init__(FloatField(), FloatField(), **kwargs)
    def schema_json(self):
        super_schema = super(GeoField, self).schema_json()
        return dict(**super_schema)


class EnumField(Field):
    ''' Represents a single value out of a list of possible values, all
        of the same type. == is used for comparison

        **Example**: ``EnumField(IntField(), 4, 6, 7)`` would accept anything
        in ``(4, 6, 7)`` as a value.  It would not accept ``5``.
        '''

    valid_modifiers = SCALAR_MODIFIERS

    def __init__(self, item_type, *values, **kwargs):
        ''' :param item_type: Instance of :class:`Field` to use for validation, and (un)wrapping
            :param values: Possible values.  ``item_type.is_valid_wrap(value)`` should be ``True``
        '''
        super(EnumField, self).__init__(**kwargs)
        self.item_type = item_type
        self.values = values
        # Jan 22, 2011: Commenting this out.  We already check that the value
        # is the right type, and that it is equal to one of the enum values.
        # If those are true, the enum values are the right type.  If we do it
        # now it causes validation issues in some cases with the
        # string-reference document fields
        #
        # for value in values:
        #     self.item_type.validate_wrap(value)
    def schema_json(self):
        super_schema = super(EnumField, self).schema_json()
        return dict(item_type=self.item_type.schema_json(),
                    values=[self.item_type.wrap(v) for v in self.values],
                    **super_schema)

    def set_parent_on_subtypes(self, parent):
        self.item_type._set_parent(parent)

    def validate_wrap(self, value):
        ''' Checks that value is valid for `EnumField.item_type` and that
            value is one of the values specified when the EnumField was
            constructed '''
        self.item_type.validate_wrap(value)

        if value not in self.values:
            self._fail_validation(value, 'Value was not in the enum values')

    def validate_unwrap(self, value):
        ''' Checks that value is valid for `EnumField.item_type`.

            .. note ::
                Since checking the value itself is not possible until is is
                actually unwrapped, that check is done in :func:`EnumField.unwrap`'''
        self.item_type.validate_unwrap(value)

    def wrap(self, value):
        ''' Validate and wrap value using the wrapping function from
            ``EnumField.item_type``
        '''
        self.validate_wrap(value)
        return self.item_type.wrap(value)

    def unwrap(self, value, session=None):
        ''' Unwrap value using the unwrap function from ``EnumField.item_type``.
            Since unwrap validation could not happen in is_valid_wrap, it
            happens in this function.'''
        self.validate_unwrap(value)
        value = self.item_type.unwrap(value, session=session)
        for val in self.values:
            if val == value:
                return val
        self._fail_validation(value, 'Value was not in the enum values')


class AnythingField(Field):
    ''' A field that passes through whatever is set with no validation.  Useful
        for free-form objects '''

    valid_modifiers = ANY_MODIFIER
    def schema_json(self):
        return super(AnythingField, self).schema_json()

    def wrap(self, value):
        ''' Always returns the value passed in'''
        return value

    def unwrap(self, value, session=None):
        ''' Always returns the value passed in'''
        return value

    def validate_unwrap(self, value):
        ''' Always passes'''
        pass
    def validate_wrap(self, value):
        ''' Always passes'''
        pass

class ObjectIdField(Field):
    ''' pymongo Object ID object.  Currently this is probably too strict.  A
        string version of an ObjectId should also be acceptable'''

    valid_modifiers = SCALAR_MODIFIERS

    def __init__(self, session=None, auto=False, **kwargs):
        if auto:
            kwargs['default_f'] = lambda : ObjectId()
        super(ObjectIdField, self).__init__(**kwargs)
    def schema_json(self):
        super_schema = super(ObjectIdField, self).schema_json()
        return dict(auth=self.auto, **super_schema)

    # def set_default(self, value):
    #     super(ObjectIdField, self).set_default(value)
    # def get_default(self):
    #     if self.auto:
    #         self.set_default(ObjectId())
    #     return super(ObjectIdField, self).get_default()
    # default = property(get_default, set_default)

    def gen(self):
        """ Helper method to create a new ObjectId """
        return ObjectId()

    def validate_wrap(self, value):
        ''' Checks that ``value`` is a pymongo ``ObjectId`` or a string
            representation of one'''
        if (not isinstance(value, ObjectId)
                and not isinstance(value, basestring)
                and not isinstance(value, bytes)
            ):
            self._fail_validation_type(value, ObjectId)
        if isinstance(value, ObjectId):
            return
        #: bytes
        if len(value) == 12:
            return
        # hex
        if len(value) != 24:
            self._fail_validation(value, 'hex object ID is the wrong length')

    def wrap(self, value, session=None):
        ''' Validates that ``value`` is an ObjectId (or hex representation
            of one), then returns it '''
        self.validate_wrap(value)
        if isinstance(value, bytes) or isinstance(value, basestring):
            return ObjectId(value)
        return value

    def unwrap(self, value, session=None):
        ''' Validates that ``value`` is an ObjectId, then returns it '''
        self.validate_unwrap(value)
        return value

class ComputedField(Field):
    ''' A computed field is generated based on an object's other values.  It
        will generally be created with the @computed_field decorator, but
        can be passed an arbitrary function.

        The function should take a dict which will contains keys with the names
        of the dependencies mapped to their values.

        The computed value is recalculated every the field is accessed unless
        the one_time field is set to True.

        Example::

            >>> class SomeDoc(Document):
            ...     @computed_field
            ...     def last_modified(obj):
            ...         return datetime.datetime.utcnow()


        .. warning::
            The computed field interacts in an undefined way with partially loaded
            documents right now.  If using this class watch out for strange behaviour.
    '''

    valid_modifiers = SCALAR_MODIFIERS

    auto = True
    def __init__(self,
            computed_type,
            fun,
            one_time=False,
            deps=None,
            **kwargs):
        ''' :param fun: the function to compute the value of the computed field
            :param computed_type: the type to use when wrapping the computed field
            :param deps: the names of fields on the current object which should be \
                passed in to compute the value
        '''
        super(ComputedField, self).__init__(**kwargs)
        self.computed_type = computed_type
        if deps is None:
            deps = set()
        self.deps = set(deps)
        self.fun = fun
        self.one_time = one_time
        self.__cached_value = UNSET

    def schema_json(self):
        super_schema = super(ComputedField, self).schema_json()
        return dict(computed_type=self.computed_type.schema_json(),
                    one_time=self.one_time,
                    deps=list(self.deps), **super_schema)


    def __get__(self, instance, owner):
        # class method
        if instance is None:
            return QueryField(self)

        obj_value = instance._values[self._name]
        if obj_value.set and self.one_time:
            return obj_value.value
        computed_value = self.compute_value(instance)
        if self.one_time:
            self.set_value(instance, computed_value)
        return computed_value

    def __set__(self, instance, value):
        obj_value = instance._values[self._name]
        if obj_value.set and self.one_time:
            raise BadValueException(self._name, value, 'Cannot set a one-time field once it has been set')
        super(ComputedField, self).__set__(instance, value)

    def set_parent_on_subtypes(self, parent):
        self.computed_type._set_parent(parent)

    def dirty_ops(self, instance):
        dirty = False
        for dep in self.deps:
            dep_value = instance._values[dep._name]
            if dep_value.dirty:
                dirty = True
                break
        else:
            if len(self.deps) > 0:
                return {}
        # make sure we recompute if this is a recompute-on-save
        value = getattr(instance, self._name)

        return {
            self.on_update : {
                self._name : self.wrap(value)
            }
        }

    def compute_value(self, doc):
        args = {}
        for dep in self.deps:
            args[dep._name] = getattr(doc, dep._name)
        value = self.fun(args)
        try:
            self.computed_type.validate_wrap(value)
        except BadValueException as bve:
            self._fail_validation(value, 'Computed Function return a bad value', cause=bve)
        return value

    def wrap_value(self, value):
        ''' A function used to wrap a value used in a comparison.  It will
            first try to wrap as the sequence's sub-type, and then as the
            sequence itself'''
        return self.computed_type.wrap_value(value)

    def validate_wrap(self, value):
        ''' Check that ``value`` is valid for unwrapping with ``ComputedField.computed_type``'''
        try:
            self.computed_type.validate_wrap(value)
        except BadValueException as bve:
            self._fail_validation(value, 'Bad value for computed field', cause=bve)

    def validate_unwrap(self, value):
        ''' Check that ``value`` is valid for unwrapping with ``ComputedField.computed_type``'''
        try:
            self.computed_type.validate_unwrap(value)
        except BadValueException as bve:
            self._fail_validation(value, 'Bad value for computed field', cause=bve)

    def wrap(self, value):
        ''' Validates ``value`` and wraps it with ``ComputedField.computed_type``'''
        self.validate_wrap(value)
        return self.computed_type.wrap(value)

    def unwrap(self, value, session=None):
        ''' Validates ``value`` and unwraps it with ``ComputedField.computed_type``'''
        self.validate_unwrap(value)
        return self.computed_type.unwrap(value, session=session)

class computed_field(object):
    def __init__(self, computed_type, deps=None, **kwargs):
        self.computed_type = computed_type
        self.deps = deps
        self.kwargs = kwargs

    def __call__(self, fun):
        return ComputedField(self.computed_type, fun, deps=self.deps, **self.kwargs)

def CreatedField(name='created', tz_aware=False, **kwargs):
    ''' A shortcut field for creation time.  It sets the current date and time
        when it enters the database and then doesn't update on further saves.

        If you've used the Django ORM, this is the equivalent of auto_now_add

        :param tz_aware: If this is True, the value will be returned in the
                         local time of the session.  It is always saved in UTC
    '''
    @computed_field(DateTimeField(), one_time=True, **kwargs)
    def created(obj):
        if tz_aware:
            import pytz
            return pytz.utc.localize(datetime.utcnow())
        return datetime.utcnow()
    created.__name__ = name
    return created

class ModifiedField(DateTimeField):
    ''' A shortcut field for modified time.  It sets the current date and time
        when it enters the database and then updates when the document is
        saved or updated

        If you've used the Django ORM, this is the equivalent of auto_now

        **WARNINGS**: When this field's parent object is sent to the database
        its modified time is set. The local copy is not updated for technical
        reasons. Hopefully this will not be the case in the future.

        :param tz_aware: If this is True, the value will be returned in the
                         local time of the session.  It is always saved in UTC
    '''
    def __init__(self, tz_aware=False, **kwargs):
        if 'use_tz' not in kwargs:
            kwargs['use_tz'] = tz_aware
        kwargs['default_f'] = lambda: self.__value()
        super(ModifiedField, self).__init__(**kwargs)

    def __value(self):
        if self.use_tz:
            import pytz
            return pytz.utc.localize(datetime.utcnow())
        return datetime.utcnow()

    def wrap(self, obj):
        value = self.__value()
        return value

    def __get__(self, instance, owner):
        # class method
        if instance is None:
            return QueryField(self)

        obj_value = instance._values[self._name]
        if obj_value.set:
            return obj_value.value
        value = self.__value()
        self.set_value(instance, value)
        return value
