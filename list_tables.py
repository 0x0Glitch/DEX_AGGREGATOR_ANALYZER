import psycopg2

# Connection parameters
DATABASE_URL = "postgresql://postgres.pmmawkmekmzgqoghnxcv:anshuman1@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"

def list_all_tables():
    conn = None
    cursor = None
    try:
        # Connect to database
        print("Connecting to database...")
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # List all tables in all schemas
        print("Retrieving all tables...")
        cursor.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name;
        """)
        
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in the database.")
            return
            
        print("\nFound tables:")
        print("-" * 50)
        print(f"{'Schema':<20} {'Table':<30}")
        print("-" * 50)
        
        for schema, table in tables:
            print(f"{schema:<20} {table:<30}")
            
            # If this is a public schema table, let's also get its columns
            if schema == 'public':
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table}'
                    ORDER BY ordinal_position;
                """)
                columns = cursor.fetchall()
                print(f"  Columns:")
                for col_name, col_type in columns:
                    print(f"    - {col_name} ({col_type})")
                print()
        
    except Exception as e:
        print(f"Error: {str(e)}")
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            print("\nConnection closed")

if __name__ == "__main__":
    print("=== Database Table List ===\n")
    list_all_tables() 