# frontend/app.py
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog
import json

class DynamoDBCloneApp:
    def __init__(self, master, system):
        self.master = master
        self.system = system
        master.title("Advanced DynamoDB Clone")
        master.geometry("1000x700")

        # Notebook for different operations
        self.notebook = ttk.Notebook(master)
        self.notebook.pack(expand=True, fill='both')

        # Create all tabs
        self._create_put_tab()
        self._create_get_tab()
        self._create_secondary_index_tab()
        self._create_nodes_tab()
        self._create_load_balancing_tab()
        self._create_backup_tab()

    def _create_put_tab(self):
        # Put Tab
        self.put_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.put_frame, text="Put")

        # Key input
        tk.Label(self.put_frame, text="Key:").pack()
        self.key_entry = tk.Entry(self.put_frame, width=50)
        self.key_entry.pack()

        # Value input (JSON)
        tk.Label(self.put_frame, text="Value (JSON):").pack()
        self.value_text = tk.Text(self.put_frame, height=10, width=50)
        self.value_text.pack()

        # TTL input
        tk.Label(self.put_frame, text="TTL (seconds, optional):").pack()
        self.ttl_entry = tk.Entry(self.put_frame, width=10)
        self.ttl_entry.pack()

        # Put button
        put_button = tk.Button(self.put_frame, text="Put", command=self._put_data)
        put_button.pack(pady=10)

    def _create_get_tab(self):
        # Get Tab
        self.get_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.get_frame, text="Get")

        # Key input for get
        tk.Label(self.get_frame, text="Key:").pack()
        self.get_key_entry = tk.Entry(self.get_frame, width=50)
        self.get_key_entry.pack()

        # Get button
        get_button = tk.Button(self.get_frame, text="Get", command=self._get_data)
        get_button.pack(pady=10)

        # Result display
        self.get_result_text = tk.Text(self.get_frame, height=10, width=50)
        self.get_result_text.pack()

    def _create_secondary_index_tab(self):
        # Secondary Index Tab
        self.secondary_index_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.secondary_index_frame, text="Query Index")

        # Condition inputs
        tk.Label(self.secondary_index_frame, text="Add Query Condition:").pack()
        
        # Condition frame
        condition_frame = tk.Frame(self.secondary_index_frame)
        condition_frame.pack()

        # Attribute input
        tk.Label(condition_frame, text="Attribute:").grid(row=0, column=0)
        self.condition_attr_entry = tk.Entry(condition_frame, width=20)
        self.condition_attr_entry.grid(row=0, column=1)

        # Value input
        tk.Label(condition_frame, text="Value:").grid(row=0, column=2)
        self.condition_value_entry = tk.Entry(condition_frame, width=20)
        self.condition_value_entry.grid(row=0, column=3)

        # Conditions listbox
        self.conditions_listbox = tk.Listbox(self.secondary_index_frame, width=50)
        self.conditions_listbox.pack(pady=10)

        # Buttons frame
        buttons_frame = tk.Frame(self.secondary_index_frame)
        buttons_frame.pack()

        # Add condition button
        add_condition_btn = tk.Button(buttons_frame, text="Add Condition", command=self._add_condition)
        add_condition_btn.pack(side=tk.LEFT, padx=5)

        # Clear conditions button
        clear_conditions_btn = tk.Button(buttons_frame, text="Clear Conditions", command=self._clear_conditions)
        clear_conditions_btn.pack(side=tk.LEFT, padx=5)

        # Query button
        query_btn = tk.Button(buttons_frame, text="Query", command=self._query_secondary_index)
        query_btn.pack(side=tk.LEFT, padx=5)

        # Results display
        self.query_results_text = tk.Text(self.secondary_index_frame, height=10, width=50)
        self.query_results_text.pack(pady=10)

        # Store conditions
        self.current_conditions = {}

    def _create_nodes_tab(self):
        # Nodes Tab
        self.nodes_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.nodes_frame, text="Nodes")

        # Add Node section
        tk.Label(self.nodes_frame, text="Add Node").pack()
        self.node_name_entry = tk.Entry(self.nodes_frame, width=30)
        self.node_name_entry.pack()
        
        add_node_button = tk.Button(self.nodes_frame, text="Add Node", command=self._add_node)
        add_node_button.pack(pady=10)

        # Node list
        self.node_listbox = tk.Listbox(self.nodes_frame, width=50)
        self.node_listbox.pack()
        self._update_node_list()

    def _create_load_balancing_tab(self):
        # Load Balancing Tab
        self.load_balancing_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.load_balancing_frame, text="Load Balancing")

        # Current node loads
        tk.Label(self.load_balancing_frame, text="Current Node Loads:").pack()
        
        # Loads display
        self.node_loads_text = tk.Text(self.load_balancing_frame, height=10, width=50)
        self.node_loads_text.pack(pady=10)

        # Balance load button
        balance_load_btn = tk.Button(self.load_balancing_frame, text="Balance Load", command=self._balance_load)
        balance_load_btn.pack(pady=10)

        # Refresh loads button
        refresh_loads_btn = tk.Button(self.load_balancing_frame, text="Refresh Loads", command=self._refresh_node_loads)
        refresh_loads_btn.pack(pady=10)

    def _create_backup_tab(self):
        # Backup Tab
        self.backup_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.backup_frame, text="Backup & Restore")

        # Create snapshot button
        create_snapshot_btn = tk.Button(self.backup_frame, text="Create Snapshot", command=self._create_snapshot)
        create_snapshot_btn.pack(pady=10)

        # Snapshots list
        tk.Label(self.backup_frame, text="Available Snapshots:").pack()
        self.snapshots_listbox = tk.Listbox(self.backup_frame, width=50)
        self.snapshots_listbox.pack(pady=10)

        # Buttons frame
        buttons_frame = tk.Frame(self.backup_frame)
        buttons_frame.pack()

        # Refresh snapshots button
        refresh_snapshots_btn = tk.Button(buttons_frame, text="Refresh Snapshots", command=self._refresh_snapshots)
        refresh_snapshots_btn.pack(side=tk.LEFT, padx=5)

        # Restore snapshot button
        restore_snapshot_btn = tk.Button(buttons_frame, text="Restore Snapshot", command=self._restore_snapshot)
        restore_snapshot_btn.pack(side=tk.LEFT, padx=5)

        # Initial refresh of snapshots
        self._refresh_snapshots()

    def _put_data(self):
        key = self.key_entry.get()
        try:
            value = json.loads(self.value_text.get("1.0", tk.END))
        except json.JSONDecodeError:
            messagebox.showerror("Error", "Invalid JSON")
            return

        ttl = self.ttl_entry.get()
        ttl = int(ttl) if ttl else None

        try:
            self.system.quorum_put(key, value, ttl)
            messagebox.showinfo("Success", f"Put key: {key}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _get_data(self):
        key = self.get_key_entry.get()
        try:
            result = self.system.quorum_get(key)
            self.get_result_text.delete('1.0', tk.END)
            self.get_result_text.insert(tk.END, json.dumps(result, indent=2))
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _add_condition(self):
        attr = self.condition_attr_entry.get()
        value = self.condition_value_entry.get()
        
        if not attr or not value:
            messagebox.showerror("Error", "Both attribute and value are required")
            return
        
        # Add to conditions
        self.current_conditions[attr] = value
        
        # Update listbox
        self.conditions_listbox.insert(tk.END, f"{attr}: {value}")
        
        # Clear entries
        self.condition_attr_entry.delete(0, tk.END)
        self.condition_value_entry.delete(0, tk.END)

    def _clear_conditions(self):
        self.current_conditions.clear()
        self.conditions_listbox.delete(0, tk.END)

    def _query_secondary_index(self):
        try:
            results = self.system.query_secondary_index(self.current_conditions)
            
            # Clear previous results
            self.query_results_text.delete('1.0', tk.END)
            print(self.current_conditions)
            print(results)
            # Display results
            if results:
                self.query_results_text.insert(tk.END, "Matching Keys:\n")
                for key in results:
                    # Fetch full data for each key
                    data = self.system.quorum_get(key)
                    self.query_results_text.insert(tk.END, f"{key}: {json.dumps(data)}\n")
            else:
                self.query_results_text.insert(tk.END, "No matching results found.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _add_node(self):
        node_name = self.node_name_entry.get()
        if not node_name:
            messagebox.showerror("Error", "Node name cannot be empty")
            return
        
        try:
            from backend.node import Node
            new_node = Node(node_name)
            self.system.consistent_hashing.add_node(new_node)
            self._update_node_list()
            messagebox.showinfo("Success", f"Added node: {node_name}")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _update_node_list(self):
        self.node_listbox.delete(0, tk.END)
        for node_id in self.system.consistent_hashing.nodes:
            self.node_listbox.insert(tk.END, node_id)

    def _balance_load(self):
        try:
            self.system.balance_load()
            messagebox.showinfo("Success", "Load balanced successfully")
            self._refresh_node_loads()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _refresh_node_loads(self):
        # Clear previous loads
        self.node_loads_text.delete('1.0', tk.END)
        
        # Get and display current node loads
        loads = self.system.load_balancer.get_node_loads()
        for node, load in loads.items():
            self.node_loads_text.insert(tk.END, f"{node}: {load} items\n")

    def _create_snapshot(self):
        try:
            snapshot_path = self.system.create_snapshot()
            messagebox.showinfo("Success", f"Snapshot created: {snapshot_path}")
            self._refresh_snapshots()
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _refresh_snapshots(self):
        # Clear previous snapshots
        self.snapshots_listbox.delete(0, tk.END)
        
        # Get and display available snapshots
        snapshots = self.system.list_snapshots()
        for snapshot in snapshots:
            self.snapshots_listbox.insert(tk.END, snapshot)

    def _restore_snapshot(self):
        # Get selected snapshot
        selected = self.snapshots_listbox.curselection()
        if not selected:
            messagebox.showerror("Error", "Please select a snapshot to restore")
            return
        
        snapshot_name = self.snapshots_listbox.get(selected[0])
        
        try:
            # Confirm restore
            confirm = messagebox.askyesno("Confirm", f"Restore snapshot {snapshot_name}?")
            if confirm:
                restored_data = self.system.load_snapshot(snapshot_name)
                messagebox.showinfo("Success", f"Snapshot {snapshot_name} restored")
        except Exception as e:
            messagebox.showerror("Error", str(e))