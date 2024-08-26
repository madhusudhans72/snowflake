
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
cursor.execute("CREATE OR REPLACE DATABASE check_DB;")
cursor.execute("GRANT OWNERSHIP ON DATABASE check_DB TO ROLE HOL_ROLE;")

# Use the newly created database
cursor.execute("USE DATABASE check_DB;")
cursor.execute("CREATE OR REPLACE SCHEMA test_schema;")
cursor.execute("USE SCHEMA test_schema;")
# Create a Parque file format
# cursor.execute("""
#     CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
#     TYPE = PARQUET
#     COMPRESSION = SNAPPY;
# """)

# Create the stage
cursor.execute("CREATE OR REPLACE STAGE my_stage;")

# Upload the Parquet file to the stage
local_file_path = 'cities*'
stage_location = '@my_stage/files/'
put_command = f"PUT file://{local_file_path} {stage_location} AUTO_COMPRESS=FALSE"
cursor.execute(put_command)
cursor.execute("LIST @my_stage/files/")
    
    # Fetch and print the results
results = cursor.fetchall()
for row in results:
    print(row)
# Close the cursor and connection


try:
    # Create a table to store the data
    create_table_command = """
    CREATE OR REPLACE TABLE cities (
        id INT,
        name STRING,
        age INT
    );
    """
    
    # Execute the CREATE TABLE command
    cursor.execute(create_table_command)
    print("Table created successfully.")

except snowflake.connector.errors.ProgrammingError as e:
    print("Error creating table:", e)

try:
    # Copy data from the Parquet file to the table
    copy_command = """
    COPY INTO cities
    FROM @my_stage/cities2.parquet
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    FILE_FORMAT = (TYPE = 'PARQUET');
    """
    cursor.execute(copy_command)
    print("Data copied into table successfully.")

except snowflake.connector.errors.ProgrammingError as e:
    print("Error copying data:", e)



    # Create or replace task
cursor.execute("""
        CREATE OR REPLACE TASK my_task
        WAREHOUSE = HOL_WH
        SCHEDULE = '1 MINUTE'
        AS
        COPY INTO cities
        FROM @my_stage/files/cities2.parquet
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        FILE_FORMAT = (TYPE = 'PARQUET');
    """)



cursor.close()
conn.close()
