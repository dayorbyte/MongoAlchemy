from __future__ import print_function
from mongoalchemy.py3compat import *

from nose.tools import *
from mongoalchemy.session import Session
from mongoalchemy.document import Document, Index, FieldNotRetrieved
from mongoalchemy.fields import *
from mongoalchemy.query import BadQueryException, Query, BadResultException
from test.util import known_failure

class BaseDoc(Document):
    pass


class SchemaTestDoc(BaseDoc):
    intfield = IntField()
    strfield = StringField()
    dtfield = DateTimeField()
    tuplefield = TupleField(AnythingField(), IntField())
    geo = GeoField()
    enum = EnumField(AnythingField(), "a")
    anyf = AnythingField()
    default_field = IntField(default=2)
    defaultf_field = IntField(default_f=lambda : 3)
    modified = ModifiedField()
    created = CreatedField()
    dict_field = DictField(AnythingField())
    list_field = ListField(AnythingField())
    set_field = SetField(AnythingField())
    kv_field = KVField(AnythingField(), AnythingField())

class DocFieldDoc(Document):
    docfield = DocumentField(SchemaTestDoc)
    sref = SRefField(SchemaTestDoc)
    ref = RefField(SchemaTestDoc)

def contains(value, wanted):
    for k, v in wanted.items():
        assert value[k] == v, (k, k in value, value.get(k))

def test_schema():
    schema = SchemaTestDoc.schema_json()

    assert schema['config_full_name'] is None
    assert schema['config_polymorphic'] is None
    assert schema['config_namespace'] == 'global'
    assert schema['config_polymorphic_identity'] is None
    assert schema['config_extra_fields'] == 'error'

    fields = schema['fields']

    contains(fields['mongo_id'], {'db_field': '_id', 'auth': False, 'type': 'ObjectIdField'})
    contains(fields['intfield'], {'default_unset': True, 'db_field': None, 'unwrap_validator': False, 'max_value': None, 'min_value': None, 'required': True, 'wrap_validator': False, 'allow_none': False, 'validator_set': False, 'ignore_missing': False, 'type': 'IntField'})
    contains(fields['strfield'], {'type': 'StringField'})
    contains(fields['dtfield'] , {'min_date': None, 'max_date': None, 'type': 'DateTimeField', 'use_tz': False})
    # Tuple
    contains(fields['tuplefield'], {'type': 'TupleField'})
    contains(fields['tuplefield']['types'][0], {'type': 'AnythingField'})
    contains(fields['tuplefield']['types'][1], {'type': 'IntField'})
    # Geo
    contains(fields['geo']['types'][0], {'type': 'FloatField'})
    contains(fields['geo']['types'][1], {'type': 'FloatField'})
    contains(fields['geo'], {'type': 'GeoField'})

    # contains(fields['enum'][], {'type': 'AnythingField'})
    contains(fields['enum'], {'values': [u'a']})
    assert fields['enum']['item_type']['type'] == 'AnythingField', fields['enum']['item_type']
    contains(fields['enum'], {'type':'EnumField'})

    contains(fields['anyf'],  {'type': 'AnythingField'})
    # assert False,

    contains(fields['default_field'], {'default' : 2})
    'function' in fields['defaultf_field'].get('default_f', '')

    # Modified Field
    mod = fields['modified']
    contains(mod, {'type': 'ModifiedField'})

    # Computed Field
    mod = fields['created']
    contains(mod, {'type': 'ComputedField', 'one_time': True, 'deps':[]})
    contains(mod['computed_type'], {'type': 'DateTimeField'})


    # Mapping Fields
    assert fields['dict_field']['value_type']['type'] == 'AnythingField'
    contains(fields['dict_field'], {'type':'DictField'})

    assert fields['kv_field']['value_type']['type'] == 'AnythingField'
    assert fields['kv_field']['key_type']['type'] == 'AnythingField'
    contains(fields['kv_field'], {'type':'KVField'})

    assert fields['list_field']['item_type']['type'] == 'AnythingField'
    contains(fields['list_field'], {'type':'ListField'})

    assert fields['set_field']['item_type']['type'] == 'AnythingField'
    contains(fields['set_field'], {'type':'SetField'})

def test_docfield():
    schema = DocFieldDoc.schema_json()
    fields = schema['fields']
    contains(fields['docfield'], {'type': 'DocumentField', 'subtype': 'global:SchemaTestDoc'})

    print(fields['ref'])
    assert fields['ref']['subtype']['subtype'] == 'global:SchemaTestDoc'
    assert fields['sref']['subtype']['subtype'] == 'global:SchemaTestDoc'
    # assert False, fields['ref']


