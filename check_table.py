import psycopg2

# Connection parameters
DATABASE_URL = "postgresql://postgres.pmmawkmekmzgqoghnxcv:anshuman1@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
TABLE_NAME = "Kyberswap"

def check_table_structure():
    try:
        # Connect to database
        print("Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Get column information
        print(f"\nGetting column information for table '{TABLE_NAME}'...")
        cur.execute(f"""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = '{TABLE_NAME}'
            ORDER BY ordinal_position;
        """)
        
        columns = cur.fetchall()
        
        print("\nTable structure:")
        print("-" * 60)
        print(f"{'Column Name':<30} {'Data Type':<20} {'Max Length'}")
        print("-" * 60)
        
        for col in columns:
            print(f"{col[0]:<30} {col[1]:<20} {col[2] if col[2] else ''}")
            
    except Exception as e:
        print(f"Error: {str(e)}")
        
    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
            print("\nConnection closed")

if __name__ == "__main__":
    print("=== Checking Table Structure ===\n")
    check_table_structure() 