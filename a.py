import psycopg2
from datetime import datetime

# Connection parameters

def test_connection():
    try:
        # Establish connection
        print("Attempting to connect to Supabase...")
        conn = psycopg2.connect(DATABASE_URL)
        
        # Create a cursor
        cur = conn.cursor()
        
        # Test the connection by executing a simple query
        print("Testing connection with a simple query...")
        cur.execute("SELECT current_timestamp;")
        
        # Fetch the result
        result = cur.fetchone()
        print(f"Current database timestamp: {result[0]}")
        
        # Create a test table if it doesn't exist
        print("\nCreating a test table if it doesn't exist...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS test_connection (
                id SERIAL PRIMARY KEY,
                message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Insert a test record
        test_message = f"Connection test successful at {datetime.now()}"
        print(f"Inserting test message: {test_message}")
        cur.execute(
            "INSERT INTO test_connection (message) VALUES (%s) RETURNING id;",
            (test_message,)
        )
        
        # Get the inserted record's ID
        inserted_id = cur.fetchone()[0]
        print(f"Successfully inserted record with ID: {inserted_id}")
        
        # Commit the transaction
        conn.commit()
        print("\nTransaction committed successfully!")
        
        # Close cursor and connection
        cur.close()
        conn.close()
        print("Connection closed.")
        
        return True
        
    except Exception as e:
        print(f"\nError: {str(e)}")
        return False
    
    finally:
        try:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()
                print("Connection resources cleaned up.")
        except:
            pass

if __name__ == "__main__":
    print("=== Supabase Connection Test ===\n")
    success = test_connection()
    print("\n=== Test Complete ===")
    print("Result:", "Success" if success else "Failed") 