# backend/backup_manager.py
import os
import json
from datetime import datetime

class BackupManager:
    """
    Manages point-in-time recovery with snapshots.
    """
    def __init__(self, file_path="snapshots/"):
        self.file_path = file_path
        os.makedirs(file_path, exist_ok=True)

    def save_snapshot(self, store, name=None):
        """
        Save a snapshot of the current store.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        name = name or f"snapshot_{timestamp}.json"
        path = os.path.join(self.file_path, name)
        
        # Convert store to dictionary if it's not already
        snapshot_data = store.data if hasattr(store, 'data') else store
        
        with open(path, "w") as f:
            json.dump(snapshot_data, f, indent=2)
        print(f"Snapshot saved: {path}")
        return path

    def load_snapshot(self, name):
        """
        Load a snapshot from a file.
        """
        path = os.path.join(self.file_path, name)
        if not os.path.exists(path):
            print(f"Snapshot {name} does not exist.")
            return {}
        
        with open(path, "r") as f:
            return json.load(f)

    def list_snapshots(self):
        """
        List all available snapshots.
        """
        return [f for f in os.listdir(self.file_path) if f.endswith('.json')]

    def delete_snapshot(self, name):
        """
        Delete a specific snapshot.
        """
        path = os.path.join(self.file_path, name)
        if os.path.exists(path):
            os.remove(path)
            print(f"Snapshot {name} deleted.")
        else:
            print(f"Snapshot {name} does not exist.")