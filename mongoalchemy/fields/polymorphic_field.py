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

    def validate_wrap(self, value, type_value):
        field = self.type_mapping[type_value]
        field.validate_wrap(value)
