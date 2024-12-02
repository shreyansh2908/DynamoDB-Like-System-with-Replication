# backend/node.py
from datetime import datetime, timedelta

class Node:
    """
    Represents a single node in the system.
    Includes TTL functionality for automatic expiration.
    """
    def __init__(self, node_id):
        self.node_id = node_id
        self.data = {}
        self.ttl_data = {}

    def put(self, key, value, ttl=None):
        self.data[key] = value
        if ttl:
            self.ttl_data[key] = datetime.now() + timedelta(seconds=ttl)

    def get(self, key):
        if key in self.ttl_data and datetime.now() > self.ttl_data[key]:
            self.delete(key)  # Remove expired data
            return None
        return self.data.get(key, None)

    def delete(self, key):
        if key in self.data:
            del self.data[key]
        if key in self.ttl_data:
            del self.ttl_data[key]

    def get_node_load(self):
        return len(self.data)