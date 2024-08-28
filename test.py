
import snowflake.connector

# Establish a connection
conn = snowflake.connector.connect(
    user='madhusudhan',
    password='Dharwad@8867168116',
    account='hjlraox-js74966',
    warehouse='HOL_WH',
    database='check_DB',
    schema='test_schema'
)

# Create a cursor object
cursor = conn.cursor()

# Create or replace database and grant ownership
def create_schemas():
    cursor.execute("CREATE DATABASE IF NOT EXISTS check_DB;")
    cursor.execute("GRANT OWNERSHIP ON DATABASE check_DB TO ROLE HOL_ROLE;")

    # Use the newly created database
    cursor.execute("USE DATABASE check_DB;")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS  test_schema;")
    cursor.execute("USE SCHEMA test_schema;")

    # Create the stage
    cursor.execute(" CREATE OR REPLACE STAGE my_stage;")

# Upload the Parquet file to the stage
def copy_parquet_to_stage():
    local_file_path = 'cities*'
    stage_location = '@my_stage/files/'
    put_command = f"PUT file://{local_file_path} {stage_location} AUTO_COMPRESS=FALSE"
    cursor.execute(put_command)
    cursor.execute("LIST @my_stage/files/")
    
    # Fetch and print the results
# results = cursor.fetchall()
# for row in results:
#     print(row)
# Close the cursor and connection
def create_staging_table():
    try:
        # Create a table only if it doesn't already exist
        create_table_command = """
        CREATE TABLE IF NOT EXISTS staging_table (
            id INT,
            name STRING,
            timestamp string
        );
        """
        # Execute the command
        cursor.execute(create_table_command)

        print("Table 'staging_table' created or already exists.")
        
    except Exception as e:
        print(f"An error occurred: {e}")

    try:
        # Copy data from the Parquet file to the table
        copy_command = """
        COPY INTO staging_table
        FROM @my_stage/files/cities2.parquet
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        FILE_FORMAT = (TYPE = 'PARQUET');
        """
        cursor.execute(copy_command)
        print("Data copied into table successfully.")

    except snowflake.connector.errors.ProgrammingError as e:
        print("Error copying data:", e)

def print_staging_data():
    try:
        # Execute a query
        cursor.execute("SELECT * FROM staging_table;")
        
        # Fetch all rows from the executed query
        rows = cursor.fetchall()
        
        # Iterate over the rows and print each row
        print("printing the content")
        for row in rows:
            print(row)
        print("printed the content")
    except Exception as e:
        print(f"An error occurred: {e}")

#select the latest entry 
def dump_to_actual_table():
    try:
        latest_date_tuple=cursor.execute("""SELECT TIMESTAMP
    FROM staging_table
    ORDER BY TIMESTAMP DESC
    LIMIT 1;
    """)
        latest_date_tuple=latest_date_tuple.fetchone()
        latest_date = latest_date_tuple[0]
        print("Latest date = ",latest_date)
        cursor.execute("""
            SELECT * 
            FROM staging_table 
            WHERE TIMESTAMP >= %s;
        """, (latest_date))
        rows = cursor.fetchall()
        
        # Iterate over the rows and print each row
        print("printing the content")
        for row in rows:
            print(row)
        print("printed the content")
    except Exception as e:
        print(f"An error occurred: {e}")

def actual_table():
    try:
        # Create a table only if it doesn't already exist
        create_table_command = """
        CREATE TABLE IF NOT EXISTS actual_table (
            id INT,
            name STRING,
            timestamp string
        );
        """
        # Execute the command
        cursor.execute(create_table_command)

        print("Table 'actual_table' created or already exists.")
        
    except Exception as e:
        print(f"An error occurred: {e}")

    latest_date_tuple=cursor.execute("""SELECT TIMESTAMP
    FROM actual_table
    ORDER BY TIMESTAMP DESC
    LIMIT 1;
    """)
    try:
        
        latest_date_tuple=latest_date_tuple.fetchone()
        latest_date = latest_date_tuple[0]
        print("Latest date = ",latest_date)
    except Exception as e:
        print(f"An error occurred: {e}")
        latest_date='00:00:00'
    cursor.execute("""
        INSERT INTO actual_table (id, name, timestamp)
        SELECT s.id, s.name, s.timestamp
        FROM staging_table s
        WHERE s.timestamp > %s;
        """, (latest_date))

    print("New data inserted successfully.")

def print_actual_table_content():
    try:
        # Execute a query
        cursor.execute("SELECT * FROM actual_table;")
        
        # Fetch all rows from the executed query
        rows = cursor.fetchall()
        
        # Iterate over the rows and print each row
        print("printing data from actual_table -----")
        for row in rows:
            print(row)
    except Exception as e:
        print(f"An error occurred: {e}")

#     # Create or replace task
# cursor.execute("""
#         CREATE OR REPLACE TASK my_task
#         WAREHOUSE = HOL_WH
#         SCHEDULE = '1 MINUTE'
#         AS
#         COPY INTO staging_table
#         FROM @my_stage/files/cities2.parquet
#         MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
#         FILE_FORMAT = (TYPE = 'PARQUET');
#     """)


create_schemas()
copy_parquet_to_stage()
create_staging_table()
print_staging_data()
dump_to_actual_table()
actual_table()
print_actual_table_content()
cursor.execute("DROP TABLE IF EXISTS staging_table;")

cursor.close()
conn.close()
