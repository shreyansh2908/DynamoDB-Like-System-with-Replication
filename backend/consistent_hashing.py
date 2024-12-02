# backend/consistent_hashing.py
import hashlib

class ConsistentHashing:
    """
    Handles consistent hashing for data distribution across nodes.
    """
    def __init__(self, num_replicas=3):
        self.num_replicas = num_replicas
        self.ring = {}
        self.sorted_keys = []
        self.nodes = {}

    def _hash(self, key):
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def add_node(self, node):
        """
        Adds a new node and redistributes keys.
        """
        print(f"Adding node: {node.node_id}")
        self.nodes[node.node_id] = node
        for i in range(self.num_replicas):
            node_hash = self._hash(f"{node.node_id}-{i}")
            self.ring[node_hash] = node
            self.sorted_keys.append(node_hash)
        self.sorted_keys.sort()
        self.redistribute_data()

    def remove_node(self, node_id):
        """
        Removes a node and redistributes keys.
        """
        print(f"Removing node: {node_id}")
        if node_id not in self.nodes:
            print(f"Node {node_id} does not exist.")
            return

        node = self.nodes.pop(node_id)
        for i in range(self.num_replicas):
            node_hash = self._hash(f"{node.node_id}-{i}")
            self.ring.pop(node_hash, None)
            self.sorted_keys.remove(node_hash)
        self.sorted_keys.sort()
        self.redistribute_data()

    def get_node(self, key):
        key_hash = self._hash(key)
        for node_hash in self.sorted_keys:
            if key_hash <= node_hash:
                return self.ring[node_hash]
        return self.ring[self.sorted_keys[0]]

    def redistribute_data(self, source_data=None):
        """
        Redistribute all data after adding or removing nodes.
        If `source_data` is provided, use it as the data source instead of node-local data.
        """
        print("Redistributing data...")
        all_data = source_data or {}
        
        # Collect data from all nodes if no source_data is provided
        if not source_data:
            for node in self.nodes.values():
                all_data.update(node.data)
                node.data.clear()
        
        # Redistribute data to nodes based on consistent hashing
        for key, value in all_data.items():
            node = self.get_node(key)
            node.put(key, value)