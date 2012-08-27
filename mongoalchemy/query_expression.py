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

from mongoalchemy.exceptions import BadValueException

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
    def __init__(self, name):
        self.__name = name
    def __getattr__(self, name):
        return QueryField(FreeFormField(name))
    @classmethod
    def unwrap(cls, value, *args, **kwargs):
        return value
    def get_collection_name(self):
        return self.__name
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
    
    @property
    def __cached_id(self):
        if self.__cached_id_value == None:
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
        res = []
        current = self
        
        while type(current) != type(None):
            if current.__matched_index:
                res.append('$')
            res.append(current.get_type().db_field)
            current = current._get_parent()
        return '.'.join(reversed(res))
    
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
        """ Adapted from the Mongo docs:

            > session.query(Places).filter(Places.loc.within_box(cornerA, cornerB)
        """
        return QueryExpression({
            self : {'$within' : {
                    '$box' : [corner1, corner2],
                }}
            })
    def within_radius(self, x, y, radius):
        """ Adapted from the Mongo docs:

            > session.query(Places).filter(Places.loc.within_radius(1, 2, 50)
        """
        return QueryExpression({
            self : {'$within' : {
                    '$center' : [[x, y], radius],
                }}
            })
    def within_radius_sphere(self, x, y, radius):
        """ Adapted from the Mongo docs:

            > session.query(Places).filter(Places.loc.within_radius_sphere(1, 2, 50)
        """
        return QueryExpression({
            self : {'$within' : {
                    '$centerSphere' : [[x, y], radius],
                }}
            })
    def within_polygon(self, polygon):
        """ Adapted from the Mongo docs:

            > polygonA = [ [ 10, 20 ], [ 10, 40 ], [ 30, 40 ], [ 30, 20 ] ]
            > polygonB = { a : { x : 10, y : 20 }, b : { x : 15, y : 25 }, c : { x : 20, y : 20 } }
            > session.query(Places).filter(Places.loc.within_polygon(polygonA)
            > session.query(Places).filter(Places.loc.within_polygon(polygonB)
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
        for k, v in self.obj.iteritems():
            if not isinstance(v, dict):
                ret_obj[k] = {'$ne' : v }
                continue
            num_ops = len([x for x in v if x[0] == '$'])
            if num_ops != len(v) and num_ops != 0:
                raise BadQueryException('$ operator used in field name')
            
            if num_ops == 0:
                ret_obj[k] = {'$ne' : v }
                continue
            
            for op, value in v.iteritems():
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


def flatten(obj):
    if not isinstance(obj, dict):
        return obj
    ret = {}
    for k, v in obj.iteritems():
        if not isinstance(k, basestring):
            k = str(k)
        if isinstance(v, dict):
            v = flatten(v)
        if isinstance(v, list):
            v = [flatten(x) for x in v]
        ret[k] = v
    return ret

