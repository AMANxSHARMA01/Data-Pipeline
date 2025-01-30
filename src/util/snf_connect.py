import snowflake.connector
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
class SnowflakeDBConnection:
    def __init__(self, user, password, account, warehouse, database, schema, role=None, timeout=60):
        self.user = user
        self.password = password
        self.account = account
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.timeout = timeout
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        """Establish a connection to Snowflake."""
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role,
                login_timeout=self.timeout
            )
            self.cursor = self.connection.cursor()
            logging.info("Connection established successfully.")
        except snowflake.connector.errors.DatabaseError as e:
            logging.error(f"Error connecting to Snowflake: {e}")
            raise

    def get_raw_connection(self):
        """Return the underlying raw connection object."""
        return self.connection

    def execute_query(self, query):
        """Execute a query and return results."""
        if self.connection is None:
            raise Exception("No active connection.")
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def close(self):
        """Close the connection and cursor."""
        
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logging.info("Connection closed.")

# # Example usage of the Snowflake DB Connection Wrapper
# if __name__ == "__main__":
#     try:
#         # Replace with your Snowflake credentials
#         snowflake_connection = SnowflakeDBConnection(
#             user="Amansharma74",
#             password="31OCT2001@snowflake",
#             account="AA45019.ap-southeast-1", 
#             warehouse="COMPUTE_WH",
#             database="customer",
#             schema="customer_schema",
#             role="ACCOUNTADMIN"  # Optional 
#         )
        
#         # Connect to Snowflake
#         snowflake_connection.connect()
        
#         # Example query execution
#         query = "SELECT CURRENT_VERSION()"
#         result = snowflake_connection.execute_query(query)
#         print("Snowflake Version:", result)

#         # Closing connection
#         snowflake_connection.close()

#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
