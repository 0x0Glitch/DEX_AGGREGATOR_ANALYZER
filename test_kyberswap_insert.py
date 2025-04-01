import psycopg2
from datetime import datetime
import time

# Connection parameters
DATABASE_URL = "postgresql://postgres.pmmawkmekmzgqoghnxcv:anshuman1@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
TABLE_NAME = "Kyberswap"  # This needs to be quoted in SQL

def insert_data_to_supabase(row_data: dict):
    """
    Inserts one row (dictionary) into the specified TABLE_NAME using direct PostgreSQL connection.
    The dictionary keys must match the column names in your table.
    """
    try:
        # Create connection
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Fix case-sensitive column names if needed
        fixed_data = {}
        
        # If 'timestamp' is in the row_data, convert it to 'TimeStamp'
        if 'timestamp' in row_data:
            # If TimeStamp column expects numeric, convert timestamp to Unix timestamp (seconds since epoch)
            if isinstance(row_data['timestamp'], str):
                timestamp_obj = datetime.fromisoformat(row_data['timestamp'].replace('Z', '+00:00'))
                fixed_data['TimeStamp'] = timestamp_obj.timestamp()  # Convert to Unix timestamp
            else:
                fixed_data['TimeStamp'] = row_data['timestamp']
        
        # Copy the rest of the data with correct case
        for key, value in row_data.items():
            if key != 'timestamp':
                # Keep original key case for all other columns
                fixed_data[key] = value
        
        print(f"Fixed data to insert: {fixed_data}")
        
        # Check that we have data to insert
        if not fixed_data:
            print("No valid data to insert")
            return
        
        # Prepare the column names and values - quote column names
        columns = ', '.join([f'"{k}"' for k in fixed_data.keys()])
        values = ', '.join(['%s'] * len(fixed_data))
        
        # Create insert query with quoted table name
        query = f'INSERT INTO "{TABLE_NAME}" ({columns}) VALUES ({values})'
        print(f"Query: {query}")
        print(f"Values: {list(fixed_data.values())}")
        
        # Execute the query with the values
        cur.execute(query, list(fixed_data.values()))
        
        # Commit the transaction
        conn.commit()
        print(f'Inserted data into table "{TABLE_NAME}" successfully')
        
        # Verify the data was inserted
        cur.execute(f'SELECT * FROM "{TABLE_NAME}" ORDER BY "TimeStamp" DESC LIMIT 1')
        result = cur.fetchone()
        print(f"Latest row in table: {result}")
        
        return True
        
    except Exception as e:
        print(f"Error inserting data into database: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
        return False
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
            print("Connection closed")

def test_insert():
    # Create test data
    timestamp = time.time()  # Current Unix timestamp
    
    test_data = {
        "timestamp": timestamp,  # Using Unix timestamp directly
        "DAI": 345.67,
        "WETH": 5.5,
        "USDbC": 789.12
    }
    
    print(f"Attempting to insert test data: {test_data}")
    result = insert_data_to_supabase(test_data)
    return result

if __name__ == "__main__":
    print("=== Testing Kyberswap Table Insert ===\n")
    success = test_insert()
    print("\nInsert test result:", "Success" if success else "Failed") 