# main.py
import tkinter as tk
from backend.node import Node
from backend.consistent_hashing import ConsistentHashing
from backend.replication import ReplicationManager
from backend.secondary_index import GlobalSecondaryIndex
from backend.load_balancer import LoadBalancer
from backend.backup_manager import BackupManager
from frontend.app import DynamoDBCloneApp

class DynamoDBCloneSystem:
    def __init__(self):
        # Initialize components
        self.leader = Node("Leader")
        self.followers = [Node(f"Follower{i}") for i in range(1, 3)]
        
        self.consistent_hashing = ConsistentHashing()
        self.replication_manager = ReplicationManager(
            self.leader, 
            self.followers, 
            quorum_size=2
        )
        
        # Add additional components
        self.secondary_index = GlobalSecondaryIndex()
        self.load_balancer = LoadBalancer(self.consistent_hashing)
        self.backup_manager = BackupManager()
        
        # Add initial nodes to consistent hashing
        self.consistent_hashing.add_node(self.leader)
        for follower in self.followers:
            self.consistent_hashing.add_node(follower)

        

    # def quorum_put(self, key, value, ttl=None):
    #     # Put data and index it
    #     result = self.replication_manager.quorum_put(key, value, ttl)
    #     print(f"Adding to secondary index: key={key}, value={value}")
    #     self.secondary_index.create_index("name")
    #     self.secondary_index.index_data(key, value)
    #     print(self.secondary_index)
    #     return result

    def quorum_put(self, key, value, ttl=None):
        # Put data and index it
        result = self.replication_manager.quorum_put(key, value, ttl)
        
        # Print what is being indexed
        print(f"Adding to secondary index: key={key}, value={value}")
        
        # Assuming value is a dictionary, index all attributes in value
        if isinstance(value, dict):
            # Create index for each attribute (optional if you want to index more attributes)
            for attribute in value:
                self.secondary_index.create_index(attribute)
            
            # Add key to the secondary index for each attribute's value
            self.secondary_index.index_data(key, value)
            
            print("Updated secondary index:")
            print(self.secondary_index.index)
        else:
            print("Value is not in the expected dictionary format.")
        
        return result


    def quorum_get(self, key):
        return self.replication_manager.quorum_get(key)

    def query_secondary_index(self, conditions):
        return self.secondary_index.query(conditions)

    def balance_load(self):
        self.load_balancer.balance_load()

    def create_snapshot(self, name=None):
        return self.backup_manager.save_snapshot(self.leader, name)

    def list_snapshots(self):
        return self.backup_manager.list_snapshots()

    def load_snapshot(self, name):
        snapshot_data = self.backup_manager.load_snapshot(name)
        # Redistribute loaded data
        self.consistent_hashing.redistribute_data(snapshot_data)
        return snapshot_data

def main():
    root = tk.Tk()
    system = DynamoDBCloneSystem()
    app = DynamoDBCloneApp(root, system)
    root.mainloop()

if __name__ == "__main__":
    main()