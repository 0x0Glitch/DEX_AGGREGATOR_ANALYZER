import psycopg2
from datetime import datetime

# Connection parameters
DATABASE_URL = "postgresql://postgres.pmmawkmekmzgqoghnxcv:anshuman1@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
TABLE_NAME = "ay"

def test_kyberswap_insert():
    conn = None
    cursor = None
    try:
        # Create test data
        test_data = {
            "timestamp": datetime.now().isoformat(),
            "DAI": 123.45,
            "USDbC": 678.90
        }
        
        print(f"Attempting to insert test data: {test_data}")
        
        # Connect to database
        print("Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{TABLE_NAME.lower()}');")
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print(f"Table '{TABLE_NAME}' does not exist!")
            return False
        
        # Get table columns
        print(f"Getting columns for table '{TABLE_NAME}'...")
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{TABLE_NAME.lower()}';")
        columns = [row[0] for row in cursor.fetchall()]
        print(f"Table columns: {columns}")
        
        # Filter data to include only columns that exist in the table
        filtered_data = {k: v for k, v in test_data.items() if k.lower() in [col.lower() for col in columns]}
        
        if not filtered_data:
            print("No matching columns found in table!")
            return False
            
        print(f"Filtered data: {filtered_data}")
        
        # Prepare the column names and values
        column_names = ', '.join(filtered_data.keys())
        placeholders = ', '.join(['%s'] * len(filtered_data))
        
        # Create insert query
        query = f"INSERT INTO {TABLE_NAME} ({column_names}) VALUES ({placeholders})"
        print(f"Query: {query}")
        
        # Execute the query with the values
        cursor.execute(query, list(filtered_data.values()))
        
        # Commit the transaction
        conn.commit()
        print(f"Successfully inserted test data into '{TABLE_NAME}'")
        
        # Now verify if data exists
        cursor.execute(f"SELECT * FROM {TABLE_NAME} ORDER BY timestamp DESC LIMIT 1;")
        result = cursor.fetchone()
        print(f"Latest row in table: {result}")
        
        return True
        
    except Exception as e:
        print(f"Error: {str(e)}")
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("Connection closed")

if __name__ == "__main__":
    print("=== Testing Database Insert ===")
    success = test_kyberswap_insert()
    print("Test result:", "Success" if success else "Failed") 