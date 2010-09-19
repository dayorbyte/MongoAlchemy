
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



class QueryResult(object):
    def __init__(self, cursor, type):
        self.cursor = cursor
        self.type = type
    
    def next(self):
        return self.type.unwrap(self.cursor.next())
    
    def __iter__(self):
        return self
    
    
