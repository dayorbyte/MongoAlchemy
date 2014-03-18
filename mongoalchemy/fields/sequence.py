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


class SequenceField(Field):
    ''' Base class for Fields which are an iterable collection of objects in which
        every child element is of the same type'''

    is_sequence_field = True
    valid_modifiers = LIST_MODIFIERS

    def __init__(self, item_type, min_capacity=None, max_capacity=None,
            default_empty=False, **kwargs):
        ''' :param item_type: :class:`Field` instance used for validation and (un)wrapping
            :param min_capacity: minimum number of items contained in values
            :param max_capacity: maximum number of items contained in values
            :param default_empty: the default is an empty sequence.
        '''
        super(SequenceField, self).__init__(**kwargs)
        self.item_type = item_type
        self.min = min_capacity
        self.max = max_capacity
        self.default_empty = default_empty
        if not isinstance(item_type, Field):
            raise BadFieldSpecification("List item_type is not a field!")
    def schema_json(self):
        super_schema = super(SequenceField, self).schema_json()
        return dict(item_type=self.item_type.schema_json(),
                    min_capacity=self.min,
                    max_capacity=self.max,
                    default_empty=self.default_empty, **super_schema)

    @property
    def has_subfields(self):
        ''' Returns True if the sequence's value type has subfields. '''
        return self.item_type.has_subfields

    @property
    def has_autoload(self):
        return self.item_type.has_autoload

    def set_parent_on_subtypes(self, parent):
        self.item_type._set_parent(parent)

    def subfields(self):
        ''' Returns the names of the value type's sub-fields'''
        return self.item_type.subfields()

    def _dereference(self, session, ref, allow_none=False):
        return self.item_type.dereference(session, ref, allow_none=allow_none)

    def wrap_value(self, value):
        ''' A function used to wrap a value used in a comparison.  It will
            first try to wrap as the sequence's sub-type, and then as the
            sequence itself'''
        try:
            return self.item_type.wrap_value(value)
        except BadValueException:
            pass
        try:
            return self.wrap(value)
        except BadValueException:
            pass
        self._fail_validation(value, 'Could not wrap value as the correct type.  Tried %s and %s' % (self.item_type, self))

    def child_type(self):
        ''' Returns the :class:`Field` instance used for items in the sequence'''
        return self.item_type

    def _validate_child_wrap(self, value):
        self.item_type.validate_wrap(value)

    def _validate_child_unwrap(self, value, session=None):
        if self.has_autoload:
            self.item_type.validate_unwrap(value, session=session)
        else:
            self.item_type.validate_unwrap(value)

    def _length_valid(self, value):
        if self.min is not None and len(value) < self.min:
            self._fail_validation(value, 'Value has too few elements')
        if self.max is not None and len(value) > self.max:
            self._fail_validation(value, 'Value has too many elements')

    def validate_wrap(self, value):
        ''' Checks that the type of ``value`` is correct as well as validating
            the elements of value'''
        self._validate_wrap_type(value)
        self._length_valid(value)
        for v in value:
            self._validate_child_wrap(v)

    def validate_unwrap(self, value, session=None):
        ''' Checks that the type of ``value`` is correct as well as validating
            the elements of value'''
        self._validate_unwrap_type(value)
        self._length_valid(value)
        for v in value:
            if self.has_autoload:
                self._validate_child_unwrap(v, session=session)
            else:
                self._validate_child_unwrap(v)


    def set_value(self, instance, value):
        super(SequenceField, self).set_value(instance, value)
        # TODO:2012
        # value_obj = instance._values[self._name]
        # if from_db:
        #     # loaded from db, stash it
        #     if 'orig_values' not in instance.__dict__:
        #         instance.__dict__['orig_values'] = {}
        #     instance.__dict__['orig_values'][self._name] = deepcopy(value)

    def dirty_ops(self, instance):
        obj_value = instance._values[self._name]
        ops = super(SequenceField, self).dirty_ops(instance)
        if len(ops) == 0 and obj_value.set:
            ops = {'$set': {
                self.db_field : self.wrap(obj_value.value)
            }}
        return ops


class ListField(SequenceField):
    ''' Field representing a python list.
    '''
    def __init__(self, item_type, **kwargs):
        ''' :param item_type: :class:`Field` instance used for validation and (un)wrapping
            :param min_capacity: minimum number of items contained in values
            :param max_capacity: maximum number of items contained in values
            :param default_empty: the default is an empty sequence.
        '''
        if kwargs.get('default_empty'):
            kwargs['default_f'] = list
        super(ListField, self).__init__(item_type, **kwargs)
    # def set_default(self, value):
    #     return super(ListField, self).set_default(value)
    # def get_default(self):
    #     if self.default_empty:
    #         return []
    #     return super(ListField, self).get_default()
    # default = property(get_default, set_default)

    def rel(self, ignore_missing=False):
        from mongoalchemy.fields import RefBase
        assert isinstance(self.item_type, RefBase)
        return ListProxy(self, ignore_missing=ignore_missing)

    def _validate_wrap_type(self, value):
        import types
        if not any([isinstance(value, list), isinstance(value, tuple),
            isinstance(value, types.GeneratorType)]):
            self._fail_validation_type(value, list, tuple)
    _validate_unwrap_type = _validate_wrap_type

    def wrap(self, value):
        ''' Wraps the elements of ``value`` using ``ListField.item_type`` and
            returns them in a list'''
        self.validate_wrap(value)
        return [self.item_type.wrap(v) for v in value]
    def unwrap(self, value, session=None):
        ''' Unwraps the elements of ``value`` using ``ListField.item_type`` and
            returns them in a list'''
        kwargs = {}
        if self.has_autoload:
            kwargs['session'] = session
        self.validate_unwrap(value, **kwargs)
        return [ self.item_type.unwrap(v, **kwargs) for v in value]


class SetField(SequenceField):
    ''' Field representing a python set.
    '''
    def __init__(self, item_type, **kwargs):
        ''' :param item_type: :class:`Field` instance used for validation and (un)wrapping
            :param min_capacity: minimum number of items contained in values
            :param max_capacity: maximum number of items contained in values
            :param default_empty: the default is an empty sequence.
        '''
        if kwargs.get('default_empty'):
            kwargs['default_f'] = set
        super(SetField, self).__init__(item_type, **kwargs)

    # def set_default(self, value):
    #     return super(SetField, self).set_default(value)
    # def get_default(self):
    #     if self.default_empty:
    #         return set()
    #     return super(SetField, self).get_default()
    # default = property(get_default, set_default)

    def rel(self, ignore_missing=False):
        return ListProxy(self, ignore_missing=ignore_missing)

    def _validate_wrap_type(self, value):
        if not isinstance(value, set):
            self._fail_validation_type(value, set)

    def _validate_unwrap_type(self, value):
        if not isinstance(value, list):
            self._fail_validation_type(value, list)

    def wrap(self, value):
        ''' Unwraps the elements of ``value`` using ``SetField.item_type`` and
            returns them in a set
            '''
        self.validate_wrap(value)
        return [self.item_type.wrap(v) for v in value]

    def unwrap(self, value, session=None):
        ''' Unwraps the elements of ``value`` using ``SetField.item_type`` and
            returns them in a set'''
        self.validate_unwrap(value)
        return set([self.item_type.unwrap(v, session=session) for v in value])

class ListProxy(object):
    def __init__(self, field, ignore_missing=False):
        self.field = field
        self.ignore_missing = ignore_missing
    def __get__(self, instance, owner):
        if instance is None:
            return getattr(owner, self.field._name)
        session = instance._get_session()
        def iterator():
            for v in getattr(instance, self.field._name):
                if v is None:
                    yield v
                    continue
                value = self.field._dereference(session, v,
                                               allow_none=self.ignore_missing)
                if value is None and self.ignore_missing:
                    continue
                yield value
        return iterator()

