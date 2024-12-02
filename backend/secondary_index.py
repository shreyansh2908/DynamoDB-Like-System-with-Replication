# backend/secondary_index.py
from collections import defaultdict

class GlobalSecondaryIndex:
    """
    Implements Global Secondary Index for querying on non-primary attributes.
    Supports multiple attributes for complex queries.
    """
    def __init__(self):
        self.index = defaultdict(lambda: defaultdict(list))

    def create_index(self, attribute):
        """
        Create an index for a specific attribute
        """
        self.index[attribute] = defaultdict(list)

    def index_data(self, key, data):
        """
        Index data for a given key across all indexed attributes
        """
        for attribute, value in data.items():
            if attribute in self.index:
                self.index[attribute][value].append(key)

    def query(self, conditions):
        """
        Query using multiple conditions
        Example: conditions = {"age": 25, "name": "Alice"}
        """
        results = set()
        for attribute, value in conditions.items():
            
            if attribute in self.index and value in self.index[attribute]:
                if not results:
                    results.update(self.index[attribute][value])
                else:
                    results.intersection_update(self.index[attribute][value])
        #print(results)
        return results