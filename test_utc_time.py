import psycopg2
from datetime import datetime, timezone

# Database connection parameters
DATABASE_URL = "postgresql://postgres.pmmawkmekmzgqoghnxcv:anshuman1@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
TABLE_NAME = "Kyberswap"

def test_utc_timestamp():
    try:
        # Create a UTC timestamp for today at 00:00:00
        current_time = datetime.now(timezone.utc)
        midnight_utc = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
        unix_timestamp = midnight_utc.timestamp()

        # Create test data
        test_data = {
            "TimeStamp": unix_timestamp,
            "WETH": 123.45,
            "cbETH": 456.78
        }

        print("\nTest Data:")
        print(f"Unix Timestamp: {unix_timestamp}")
        print(f"UTC Time: {midnight_utc}")

        # Connect to the database
        print("\nConnecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Prepare the insert query
        columns = ', '.join([f'"{k}"' for k in test_data.keys()])
        values = ', '.join(['%s'] * len(test_data))
        query = f'INSERT INTO "{TABLE_NAME}" ({columns}) VALUES ({values})'

        # Execute the query
        print(f"\nInserting test data...")
        cur.execute(query, list(test_data.values()))
        conn.commit()

        # Verify the insertion by retrieving the latest row
        print("\nVerifying inserted data...")
        cur.execute(f'SELECT * FROM "{TABLE_NAME}" ORDER BY "TimeStamp" DESC LIMIT 1')
        result = cur.fetchone()
        
        if result:
            # Convert the numeric timestamp back to datetime for verification
            retrieved_unix_timestamp = float(result[0])
            retrieved_datetime = datetime.fromtimestamp(retrieved_unix_timestamp, timezone.utc)
            
            print(f"\nRetrieved data:")
            print(f"Unix Timestamp: {retrieved_unix_timestamp}")
            print(f"UTC Time: {retrieved_datetime}")
            print(f"WETH: {result[1]}")
            print(f"cbETH: {result[2]}")
            
            # Verify if the timestamp is exactly at midnight UTC
            is_midnight = (
                retrieved_datetime.hour == 0 and 
                retrieved_datetime.minute == 0 and 
                retrieved_datetime.second == 0 and 
                retrieved_datetime.microsecond == 0
            )
            
            if is_midnight:
                print("\n✅ Test passed! Timestamp was stored correctly at midnight UTC")
                print(f"Stored timestamp represents: {retrieved_datetime} UTC")
            else:
                print("\n❌ Test failed! Retrieved timestamp is not at midnight UTC")
                print("Time components found:")
                print(f"Hour: {retrieved_datetime.hour} (should be 0)")
                print(f"Minute: {retrieved_datetime.minute} (should be 0)")
                print(f"Second: {retrieved_datetime.second} (should be 0)")
                print(f"Microsecond: {retrieved_datetime.microsecond} (should be 0)")
        else:
            print("\n❌ Test failed! Could not retrieve inserted data")

    except Exception as e:
        print(f"\nError during test: {str(e)}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            print("\nDatabase connection closed")

if __name__ == "__main__":
    test_utc_timestamp() 