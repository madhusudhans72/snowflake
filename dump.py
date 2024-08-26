from snowflake.snowpark import Session
import os
connection_param={

"account" : "hjlraox-js74966",
"user" : "madhusudhan",
"password" : "Dharwad@8867168116",
"role" : "HOL_ROLE",
"warehouse" : "HOL_WH",
"database" : "CHECK_DB",
"schema" : "TEST_SCHEMA",
}

session = Session.builder.configs(connection_param).create() 
print(session.get_fully_qualified_current_schema())
session.sql("USE SCHEMA TEST_SCHEMA;")
df = session.read.parquet("@my_stage/files/cities2.parquet")

# Show the DataFrame and its schema
df.show()
# List files in the stage to verify the file exists
# print("Listing files in stage:")
# session.sql("LIST @my_stage/files/").show()

# df=session.table("HOL_DB.RAW_POS.COUNTRY")
# df=df.select("*")
# print(df.show())
# print("------")
# df = session.read.parquet("my_stage/files/cities.parquet")
# df_schema= df.schema

# print("schema type ",type(df_schema))
# print("number of coloumns ",df_schema.fields)


