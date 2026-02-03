import json
import os


class MiniDB:
    """
    A simple key-value database engine that stores data in JSON format.
    
    This simulates how real databases persist data to disk, ensuring that
    data survives beyond the program's runtime.
    """
    
    def __init__(self, filename='data/db.json'):
        """
        Initialize the database engine.
        
        Args:
            filename: The file path where data will be persisted (default: 'data/db.json')
        """
        self.filename = filename
        self.data = {}
        
        # Ensure the data directory exists
        data_dir = os.path.dirname(self.filename)
        if data_dir and not os.path.exists(data_dir):
            os.makedirs(data_dir)
            print(f"Created data directory: {data_dir}")
        
        # Load existing data from disk if the file exists
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    self.data = json.load(f)
                print(f"Loaded existing database from {self.filename}")
            except json.JSONDecodeError:
                print(f"Warning: Could not parse {self.filename}, starting fresh")
                self.data = {}
        else:
            print(f"No existing database found. Starting with empty database.")
    
    def _commit(self):
        """
        Private method to persist the current state to disk.
        
        This simulates the 'commit' operation in real databases where changes
        are written to disk to ensure durability. Without this, all data would
        be lost when the program terminates (volatility of RAM vs persistence of disk).
        """
        with open(self.filename, 'w') as f:
            json.dump(self.data, f, indent=2)
    
    def insert(self, table_name, record_dict):
        """
        Insert a record into the specified table.
        
        Args:
            table_name: The name of the table (key in our dictionary)
            record_dict: A dictionary representing the record to insert
        
        Returns:
            The inserted record
        """
        # Initialize table as empty list if it doesn't exist
        if table_name not in self.data:
            self.data[table_name] = []
        
        # Append the new record to the table
        self.data[table_name].append(record_dict)
        
        # Immediately persist to disk (auto-commit for durability)
        self._commit()
        
        print(f"Inserted record into '{table_name}': {record_dict}")
        return record_dict
    
    def select_all(self, table_name):
        """
        Retrieve all records from the specified table.
        
        Args:
            table_name: The name of the table to query
        
        Returns:
            A list of all records in the table, or empty list if table doesn't exist
        """
        return self.data.get(table_name, [])
