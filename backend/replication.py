# backend/replication.py
class ReplicationManager:
    def __init__(self, leader, followers, quorum_size=None):
        self.leader = leader
        self.followers = followers
        self.quorum_size = quorum_size or len(followers) + 1

    def put(self, key, value, ttl=None):
        self.leader.put(key, value, ttl)
        for follower in self.followers:
            follower.put(key, value, ttl)

    def quorum_get(self, key):
        """
        Perform a quorum read.
        """
        results = [self.leader.get(key)]
        for follower in self.followers:
            results.append(follower.get(key))
        
        # Filter out None values and find the most common
        valid_results = [r for r in results if r is not None]
        return max(valid_results, key=valid_results.count) if valid_results else None

    def quorum_put(self, key, value, ttl=None):
        """
        Perform a quorum write.
        """
        write_count = 1  # Leader always writes
        self.leader.put(key, value, ttl)
        for follower in self.followers:
            follower.put(key, value, ttl)
            write_count += 1
            if write_count >= self.quorum_size:
                break

    def handle_conflict_resolution(self, key, values):
        """
        Handle conflicts for eventual consistency reads.
        Resolve by picking the majority.
        """
        return max(values, key=values.count)