from mongoalchemy.fields.base import *

class PolymorphicField(Field):

    def __init__(self, type_field, type_mapping, **kwargs):
        self.type_field = type_field
        self.type_mapping = type_mapping
        super(PolymorphicField, self).__init__(**kwargs)

    def wrap(self, value, type_value):
        field = self.type_mapping[type_value]
        return field.wrap(value)

    def unwrap(self, value, type_value, **kwargs):
        field = self.type_mapping[type_value]
        return field.unwrap(value, **kwargs)

    def validate_wrap(self, value, type_value=None):
        # When running __set__(..) on the field, it's not possible to get type_value.
        # So skip validate_wrap when type_value is None
        # Eventually it will be validated in wrap(..) before db calls, so it's fine.
        if type_value is None:
            return

        field = self.type_mapping[type_value]
        field.validate_wrap(value)
