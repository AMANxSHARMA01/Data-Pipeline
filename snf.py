import snowflake.connector

# Define connection parameters
conn = snowflake.connector.connect(
    user="Amansharma74",
    password="31OCT2001@snowflake",
    account="AA45019.ap-southeast-1", 
    warehouse="COMPUTE_WH",
    database="customer",
    schema="customer_schema",
    role="ACCOUNTADMIN"  # Optional
)

# Create a cursor object
cur = conn.cursor()

# Execute a query
cur.execute("SELECT CURRENT_VERSION();")

# Fetch and print the result
row = cur.fetchone()
print(f"Snowflake Version: {row[0]}")

# Close the cursor and connection
cur.close()
conn.close()
