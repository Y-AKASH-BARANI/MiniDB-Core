from .engine import MiniDB


def print_header():
    """Display welcome message and available commands."""
    print("\n" + "="*60)
    print("🗄️  Welcome to MiniDB-Core CLI")
    print("="*60)
    print("\nAvailable Commands:")
    print("  insert <table> <key=value> <key=value> ...")
    print("    Example: insert users name=John age=25 role=admin")
    print("\n  select <table>")
    print("    Example: select users")
    print("\n  exit")
    print("    Quit the database CLI")
    print("\n" + "="*60 + "\n")


def parse_insert_args(args):
    """
    Parse key=value pairs into a dictionary.
    
    Args:
        args: List of strings in format ['key=value', 'key2=value2', ...]
    
    Returns:
        Dictionary of parsed key-value pairs
    """
    record = {}
    for arg in args:
        if '=' not in arg:
            raise ValueError(f"Invalid format: '{arg}'. Expected key=value")
        
        key, value = arg.split('=', 1)  # Split only on first '='
        record[key.strip()] = value.strip()
    
    return record


def run():
    """Main REPL loop for the MiniDB CLI."""
    # Initialize the database engine
    db = MiniDB()
    
    # Display welcome and instructions
    print_header()
    
    # Start the REPL (Read-Eval-Print Loop)
    while True:
        try:
            # Get user input
            user_input = input("db> ").strip()
            
            # Skip empty inputs
            if not user_input:
                continue
            
            # Parse the command
            parts = user_input.split()
            command = parts[0].lower()
            
            # Handle EXIT command
            if command == "exit":
                print("\n👋 Goodbye! Your data has been saved.\n")
                break
            
            # Handle INSERT command
            elif command == "insert":
                if len(parts) < 3:
                    print("❌ Error: insert requires a table name and at least one key=value pair")
                    print("   Usage: insert <table> <key=value> <key=value> ...")
                    continue
                
                table_name = parts[1]
                record_args = parts[2:]
                
                # Parse key=value pairs into a dictionary
                record = parse_insert_args(record_args)
                
                # Insert into database
                db.insert(table_name, record)
                print(f"✅ Record inserted successfully!")
            
            # Handle SELECT command
            elif command == "select":
                if len(parts) < 2:
                    print("❌ Error: select requires a table name")
                    print("   Usage: select <table>")
                    continue
                
                table_name = parts[1]
                records = db.select_all(table_name)
                
                if not records:
                    print(f"📭 No records found in table '{table_name}'")
                else:
                    print(f"\n📊 Records in '{table_name}' ({len(records)} total):")
                    print("-" * 50)
                    for idx, record in enumerate(records, 1):
                        print(f"{idx}. {record}")
                    print("-" * 50)
            
            # Handle unknown commands
            else:
                print(f"❌ Unknown command: '{command}'")
                print("   Valid commands: insert, select, exit")
        
        except KeyboardInterrupt:
            print("\n\n👋 Interrupted. Goodbye!\n")
            break
        
        except Exception as e:
            print(f"❌ Error: {e}")
