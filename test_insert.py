import psycopg2
from datetime import datetime, timezone
import time

# Connection parameters
DATABASE_URL = "postgresql://postgres.pmmawkmekmzgqoghnxcv:anshuman1@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
TABLE_NAME = "Kyberswap"

def test_insert():
    try:
        # Create test data with current time
        test_data = {
            "timestamp": time.time(),
            "WETH": 123.45,
            "cbETH": 456.78,
            "wstETH": 789.12
        }
        
        # Connect to database
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Prepare data
        fixed_data = {
            "TimeStamp": float(test_data["timestamp"]),
            "WETH": test_data["WETH"],
            "cbETH": test_data["cbETH"],
            "wstETH": test_data["wstETH"]
        }
        
        print(f"Attempting to insert data: {fixed_data}")
        
        # Prepare query
        columns = ', '.join([f'"{k}"' for k in fixed_data.keys()])
        values = ', '.join(['%s'] * len(fixed_data))
        query = f'INSERT INTO "{TABLE_NAME}" ({columns}) VALUES ({values})'
        
        # Execute query
        cur.execute(query, list(fixed_data.values()))
        conn.commit()
        
        # Verify the insert
        cur.execute(f'SELECT * FROM "{TABLE_NAME}" ORDER BY "TimeStamp" DESC LIMIT 1')
        result = cur.fetchone()
        print(f"\nVerification - Latest row in database:")
        print(f"Raw result: {result}")
        
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
    print("=== Testing Insert with Correct Column Names ===\n")
    success = test_insert()
    print("\nTest result:", "Success" if success else "Failed") 