
class Query(object):
    def __init__(self, type, db):
        self.db = db
        self.type = type
        self.query = {}
    
    def __iter__(self):
        collection = self.db[self.type.get_collection_name()]
        for index in self.type.get_indexes():
            index.ensure(collection)
        cursor = collection.find(self.query)
        return QueryResult(cursor, self.type)
    
    def filter(self, *query_expressions):
        for qe in query_expressions:
            self.apply(qe)
        return self
    
    def apply(self, qe):
        for k, v in qe.obj.iteritems():
            if k not in self.query:
                self.query[k] = v
                continue
            if not isinstance(self.query[k], dict) or not isinstance(v, dict):
                raise Exception('Multiple assignments to a field must all be dicts.')
                self.query[k].update(**v)

class QueryFieldSet(object):
    def __init__(self, type, **kwargs):
        self.type = type
        self.fields = kwargs
    def __getattr__(self, name):
        if name not in self.fields:
            raise Exception('%s is not a field in %s' % (name, self.type.class_name()))
        return QueryField(name, self.fields[name])

class QueryField(object):
    def __init__(self, name, type):
        self.name = name
        self.type = type

    def __eq__(self, value):
        if not self.type.is_valid(value):
            raise Exception('Invalid "value" for query against %s.%s: %s' % (type.class_name(), name, value))
        return QueryExpression({ self.name : value })
    def __lt__(self, value):
        return self.comparator(self.type, '$lt', self.name, value)
    def __le__(self, value):
        return self.comparator(self.type, '$lte', self.name, value)
    def __ne__(self, value):
        return self.comparator(self.type, '$ne', self.name, value)
    def __gt__(self, value):
        return self.comparator(self.type, '$gt', self.name, value)
    def __ge__(self, value):
        return self.comparator(self.type, '$gte', self.name, value)
    def comparator(self, type, op, name, value):
        if not type.is_valid(value):
            raise Exception('Invalid "value" for query against %s.%s: %s' % (type.class_name(), name, value))
        return QueryExpression({
            name : {
                op : value
            }
        })

class QueryExpression(object):
    def __init__(self, obj):
        self.obj = obj
    
    

class QueryResult(object):
    def __init__(self, cursor, type):
        self.cursor = cursor
        self.type = type
    
    def next(self):
        return self.type.unwrap(self.cursor.next())
    
    def __iter__(self):
        return self
    
    
