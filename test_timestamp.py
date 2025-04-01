import psycopg2
from datetime import datetime, timezone
import time

# Connection parameters
DATABASE_URL = "postgresql://postgres.pmmawkmekmzgqoghnxcv:anshuman1@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
TABLE_NAME = "Kyberswap"

def test_timestamp_insert():
    try:
        # Create test data with current time
        test_data = {
            "timestamp": datetime.now(timezone.utc),
            "DAI": 123.45,
            "WETH": 5.5,
            "USDbC": 789.12
        }
        
        # Connect to database
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Format timestamp
        timestamp_str = test_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S GMT')
        
        # Prepare data
        data = {
            "TimeStamp": timestamp_str,
            "DAI": test_data["DAI"],
            "WETH": test_data["WETH"],
            "USDbC": test_data["USDbC"]
        }
        
        print(f"Attempting to insert with timestamp: {timestamp_str}")
        
        # Prepare query
        columns = ', '.join([f'"{k}"' for k in data.keys()])
        values = ', '.join(['%s'] * len(data))
        query = f'INSERT INTO "{TABLE_NAME}" ({columns}) VALUES ({values})'
        
        # Execute query
        cur.execute(query, list(data.values()))
        conn.commit()
        
        # Verify the insert
        cur.execute(f'SELECT "TimeStamp", "DAI", "WETH", "USDbC" FROM "{TABLE_NAME}" ORDER BY "TimeStamp" DESC LIMIT 1')
        result = cur.fetchone()
        print(f"\nVerification - Latest row in database:")
        print(f"Timestamp: {result[0]}")
        print(f"DAI: {result[1]}")
        print(f"WETH: {result[2]}")
        print(f"USDbC: {result[3]}")
        
        return True
        
    except Exception as e:
        print(f"Error: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
        return False
        
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
            print("\nConnection closed")

if __name__ == "__main__":
    print("=== Testing Timestamp Format ===\n")
    success = test_timestamp_insert()
    print("\nTest result:", "Success" if success else "Failed") 