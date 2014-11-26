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

from mongoalchemy.exceptions import BadValueException

import re

class BadQueryException(Exception):
    ''' Raised when a method would result in a query which is not well-formed.
    '''
    pass

class FreeFormField(object):
    has_subfields = True
    no_real_attributes = True

    def __init__(self, name=None):
        self.__name = name
        self.db_field = name
    def __getattr__(self, name):
        return FreeFormField(name=name)
    def __getitem__(self, name):
        return getattr(self, name)
    @classmethod
    def wrap_value(self, value):
        return value
    def subfields(self):
        return FreeFormField(name=None)
    def is_valid_wrap(*args):
        return True
    is_valid_unwrap = is_valid_wrap
    __contains__ = is_valid_wrap

class FreeFormDoc(object):
    config_default_sort = None
    def __init__(self, name):
        self.__name = name
    def __getattr__(self, name):
        return QueryField(FreeFormField(name))
    @classmethod
    def base_query(*args, **kwargs):
        return {}
    @classmethod
    def unwrap(cls, value, *args, **kwargs):
        return value
    def get_collection_name(self):
        return self.__name
    def transform_incoming(self, obj, session):
        return obj
    def get_indexes(self):
        return []
    mongo_id = FreeFormField(name='_id')

Q = FreeFormDoc('')

class QueryField(object):
    def __init__(self, type, parent=None):
        self.__type = type
        self.__parent = parent
        self.__cached_id_value = None
        self.__matched_index = False
        self.__fields_expr = True

    @property
    def fields_expression(self):
        return flatten(self.__fields_expr)

    @property
    def __cached_id(self):
        if self.__cached_id_value is None:
            self.__cached_id_value = str(self)
        return self.__cached_id_value

    def _get_parent(self):
        return self.__parent

    def get_type(self):
        ''' Returns the underlying :class:`mongoalchemy.fields.Field` '''
        return self.__type

    def matched_index(self):
        ''' Represents the matched array index on a query with objects inside
            of a list.  In the MongoDB docs, this is the ``$`` operator '''
        self.__matched_index = True
        return self

    def __getattr__(self, name):
        if not self.__type.no_real_attributes and hasattr(self.__type, name):
            return getattr(self.__type, name)

        if not self.__type.has_subfields:
            raise AttributeError(name)

        fields = self.__type.subfields()
        if name not in fields:
            raise BadQueryException('%s is not a field in %s' % (name, self.__type.sub_type()))
        return QueryField(fields[name], parent=self)

    def get_absolute_name(self):
        """ Returns the full dotted name of this field """
        res = []
        current = self

        while type(current) != type(None):
            if current.__matched_index:
                res.append('$')
            res.append(current.get_type().db_field)
            current = current._get_parent()
        return '.'.join(reversed(res))

    def startswith(self, prefix, ignore_case=False, options=None):
        """ A query to check if a field starts with a given prefix string

            **Example**: ``session.query(Spell).filter(Spells.name.startswith("abra", ignore_case=True))``

            .. note:: This is a shortcut to .regex('^' + re.escape(prefix))
                MongoDB optimises such prefix expressions to use indexes
                appropriately. As the prefix contains no further regex, this
                will be optimized by matching only against the prefix.

        """
        return self.regex('^' + re.escape(prefix), ignore_case=ignore_case, options=options)

    def endswith(self, suffix, ignore_case=False, options=None):
        """ A query to check if a field ends with a given suffix string

            **Example**: ``session.query(Spell).filter(Spells.name.endswith("cadabra", ignore_case=True))``

        """
        return self.regex(re.escape(suffix) + '$', ignore_case=ignore_case, options=options)

    def regex(self, expression, ignore_case=False, options=None):
        """ A query to check if a field matches a given regular expression
            :param ignore_case: Whether or not to ignore the case (setting this to True is the same as setting the 'i' option)
            :param options: A string of option characters, as per the MongoDB $regex operator (e.g. "imxs")

            **Example**: ``session.query(Spell).filter(Spells.name.regex(r'^abra[a-z]*cadabra$', ignore_case=True))``

        """
        regex = {'$regex' : expression}
        if options is not None:
            regex['$options'] = options
        if ignore_case:
            regex['$options'] = regex.get('$options', '') + 'i'
        expr = {
            self : regex
        }
        return QueryExpression(expr)

    def near(self, x, y, max_distance=None):
        """ Return documents near the given point
        """
        expr = {
            self : {'$near' : [x, y]}
        }
        if max_distance is not None:
            expr[self]['$maxDistance'] = max_distance
        # if bucket_size is not None:
        #     expr['$bucketSize'] = max_distance
        return QueryExpression(expr)

    def near_sphere(self, x, y, max_distance=None):
        """ Return documents near the given point using sphere distances
        """
        expr = {
            self : {'$nearSphere' : [x, y]}
        }
        if max_distance is not None:
            expr[self]['$maxDistance'] = max_distance
        return QueryExpression(expr)

    def within_box(self, corner1, corner2):
        """
            Adapted from the Mongo docs::

                session.query(Places).filter(Places.loc.within_box(cornerA, cornerB)
        """
        return QueryExpression({
            self : {'$within' : {
                    '$box' : [corner1, corner2],
                }}
            })
    def within_radius(self, x, y, radius):
        """
            Adapted from the Mongo docs::

                session.query(Places).filter(Places.loc.within_radius(1, 2, 50)
        """
        return QueryExpression({
            self : {'$within' : {
                    '$center' : [[x, y], radius],
                }}
            })
    def within_radius_sphere(self, x, y, radius):
        """
            Adapted from the Mongo docs::

                session.query(Places).filter(Places.loc.within_radius_sphere(1, 2, 50)
        """
        return QueryExpression({
            self : {'$within' : {
                    '$centerSphere' : [[x, y], radius],
                }}
            })
    def within_polygon(self, polygon):
        """
            Adapted from the Mongo docs::

                polygonA = [ [ 10, 20 ], [ 10, 40 ], [ 30, 40 ], [ 30, 20 ] ]
                polygonB = { a : { x : 10, y : 20 }, b : { x : 15, y : 25 }, c : { x : 20, y : 20 } }
                session.query(Places).filter(Places.loc.within_polygon(polygonA)
                session.query(Places).filter(Places.loc.within_polygon(polygonB)
        """
        return QueryExpression({
            self : {'$within' : {
                    '$polygon' : polygon,
                }}
            })

    def in_(self, *values):
        ''' A query to check if this query field is one of the values
            in ``values``.  Produces a MongoDB ``$in`` expression.
        '''
        return QueryExpression({
            self : { '$in' : [self.get_type().wrap_value(value) for value in values] }
        })

    def nin(self, *values):
        ''' A query to check if this query field is not one of the values
            in ``values``.  Produces a MongoDB ``$nin`` expression.
        '''
        return QueryExpression({
            self : { '$nin' : [self.get_type().wrap_value(value) for value in values] }
        })

    def exists(self, exists=True):
        ''' Create a MongoDB query to check if a field exists on a Document.
        '''
        return QueryExpression({self: {'$exists': exists}})

    def __str__(self):
        return self.get_absolute_name()

    def __repr__(self):
        return 'QueryField(%s)' % str(self)

    def __hash__(self):
        return hash(self.__cached_id)

    def __eq__(self, value):
        return self.eq_(value)
    def eq_(self, value):
        ''' Creates a query expression where ``this field == value``

            .. note:: The prefered usage is via an operator: ``User.name == value``
        '''
        if isinstance(value, QueryField):
            return self.__cached_id == value.__cached_id
        return QueryExpression({ self : self.get_type().wrap_value(value) })

    def __lt__(self, value):
        return self.lt_(value)
    def lt_(self, value):
        ''' Creates a query expression where ``this field < value``

            .. note:: The prefered usage is via an operator: ``User.name < value``
        '''
        return self.__comparator('$lt', value)

    def __le__(self, value):
        return self.le_(value)
    def le_(self, value):
        ''' Creates a query expression where ``this field <= value``

            .. note:: The prefered usage is via an operator: ``User.name <= value``
        '''
        return self.__comparator('$lte', value)

    def __ne__(self, value):
        return self.ne_(value)
    def ne_(self, value):
        ''' Creates a query expression where ``this field != value``

            .. note:: The prefered usage is via an operator: ``User.name != value``
        '''
        if isinstance(value, QueryField):
            return self.__cached_id != value.__cached_id
        return self.__comparator('$ne', value)

    def __gt__(self, value):
        return self.gt_(value)
    def gt_(self, value):
        ''' Creates a query expression where ``this field > value``

            .. note:: The prefered usage is via an operator: ``User.name > value``
        '''
        return self.__comparator('$gt', value)

    def __ge__(self, value):
        return self.ge_(value)
    def ge_(self, value):
        ''' Creates a query expression where ``this field >= value``

            .. note:: The prefered usage is via an operator: ``User.name >= value``
        '''
        return self.__comparator('$gte', value)

    def elem_match(self, value):
        ''' This method does two things depending on the context:

            1. In the context of a query expression it:

            Creates a query expression to do an $elemMatch on the selected
            field.  If the type of this field is a DocumentField the value
            can be either a QueryExpression using that Document's fields OR
            you can use a dict for raw mongo.

            See the mongo documentation for thorough treatment of
            elemMatch:
            http://docs.mongodb.org/manual/reference/operator/elemMatch/

            2. In the context of choosing fields in a query.fields() expr:

            Sets the field to use elemMatch, so only the matching elements
            of a list are used. See the mongo docs for more details:
            http://docs.mongodb.org/manual/reference/projection/elemMatch/
        '''
        self.__is_elem_match = True
        if not self.__type.is_sequence_field:
            raise BadQueryException('elem_match called on a non-sequence '
                                    'field: ' + str(self))
        if isinstance(value, dict):
            self.__fields_expr = { '$elemMatch' : value}
            return ElemMatchQueryExpression(self, {self : self.__fields_expr })
        elif isinstance(value, QueryExpression):
            self.__fields_expr = { '$elemMatch' : value.obj }
            e = ElemMatchQueryExpression(self, {
                       self : self.__fields_expr
                })
            return e
        raise BadQueryException('elem_match requires a QueryExpression '
                                '(to be typesafe) or a dict (which is '
                                'not type safe)')

    def exclude(self):
        ''' Use in a query.fields() expression to say this field should be
            excluded.  The default of fields() is to include only
            fields which are specified. This allows retrieving of "every field
            except 'foo'".
        '''
        self.__fields_expr = False
        return self

    def __comparator(self, op, value):
        return QueryExpression({
            self : {
                op : self.get_type().wrap(value)
            }
        })


class QueryExpression(object):
    ''' A QueryExpression wraps a dictionary representing a query to perform
        on a mongo collection.  The

        .. note:: There is no ``and_`` expression because multiple expressions
            can be specified to a single call of :func:`Query.filter`
    '''
    def __init__(self, obj):
        self.obj = obj
    def not_(self):
        ''' Negates this instance's query expression using MongoDB's ``$not``
            operator

            **Example**: ``(User.name == 'Jeff').not_()``

            .. note:: Another usage is via an operator, but parens are needed
                to get past precedence issues: ``~ (User.name == 'Jeff')``
            '''
        ret_obj = {}
        for k, v in self.obj.items():
            if not isinstance(v, dict):
                ret_obj[k] = {'$ne' : v }
                continue
            num_ops = len([x for x in v if x[0] == '$'])
            if num_ops != len(v) and num_ops != 0:
                raise BadQueryException('$ operator used in field name')

            if num_ops == 0:
                ret_obj[k] = {'$ne' : v }
                continue

            for op, value in v.items():
                k_dict = ret_obj.setdefault(k, {})
                not_dict = k_dict.setdefault('$not', {})
                not_dict[op] = value


        return QueryExpression(ret_obj)

    def __invert__(self):
        return self.not_()

    def __or__(self, expression):
        return self.or_(expression)

    def or_(self, expression):
        ''' Adds the given expression to this instance's MongoDB ``$or``
            expression, starting a new one if one does not exst

            **Example**: ``(User.name == 'Jeff').or_(User.name == 'Jack')``

            .. note:: The prefered usageis via an operator: ``User.name == 'Jeff' | User.name == 'Jack'``

            '''

        if '$or' in self.obj:
            self.obj['$or'].append(expression.obj)
            return self
        self.obj = {
            '$or' : [self.obj, expression.obj]
        }
        return self

class ElemMatchQueryExpression(QueryExpression):
    ''' Special QueryExpression subclass which can also be used
        in a query.fields() expression. Shouldn't be used directly.
    '''
    def __init__(self, field, obj):
        QueryExpression.__init__(self, obj)
        self._field = field
    def __str__(self):
        return str(self._field)
    def get_absolute_name(self):
        return self._field.get_absolute_name()
    @property
    def fields_expression(self):
        return self._field.fields_expression


def flatten(obj):
    if not isinstance(obj, dict):
        return obj
    ret = {}
    for k, v in obj.items():
        if not isinstance(k, basestring):
            k = str(k)
        if isinstance(v, dict):
            v = flatten(v)
        if isinstance(v, list):
            v = [flatten(x) for x in v]
        ret[k] = v
    return ret
