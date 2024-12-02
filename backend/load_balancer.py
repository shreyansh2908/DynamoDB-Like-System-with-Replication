# backend/load_balancer.py
class LoadBalancer:
    """
    Redistributes data across nodes when imbalance is detected.
    """
    def __init__(self, consistent_hashing):
        self.consistent_hashing = consistent_hashing

    def balance_load(self):
        """
        Check and redistribute data to ensure balanced node loads.
        """
        loads = {node_id: node.get_node_load() for node_id, node in self.consistent_hashing.nodes.items()}
        
        # If no nodes, return
        if not loads:
            return
        
        max_load = max(loads.values())
        min_load = min(loads.values())

        # Threshold for load imbalance (configurable)
        if max_load - min_load > 5:  
            print("Load imbalance detected. Redistributing data...")
            self.consistent_hashing.redistribute_data()

    def get_node_loads(self):
        """
        Get current load for all nodes
        """
        return {
            node_id: node.get_node_load() 
            for node_id, node in self.consistent_hashing.nodes.items()
        }