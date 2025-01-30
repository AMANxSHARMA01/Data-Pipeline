#  step 1: create current date variable
#  step 2: read data for that date
# step 3: add 2 columns(created_at,updated_at) with current date in both
#  step 4 : push the data to snowflake(staging table)
from config import settings
import datetime
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def get_file_path(delta = 0):
    today = (datetime.datetime.today()- datetime.timedelta(delta)).strftime("%Y\\%m\\%d\\")
    base_path = settings.base_path
    file_name = settings.file_name
    return base_path+today+file_name

def read_csv_file(delta):
    file_path = get_file_path(delta)
    df = pd.read_csv(file_path)
    df = df[["ID","FIRST_NAME","LAST_NAME","ADDRESS"]]
    df["START_DATE"] = datetime.datetime.today().date()
    df["END_DATE"] = datetime.datetime.strptime("31-12-9999","%d-%m-%Y").date()
    df["LAST_UPDATED"] = datetime.datetime.today().date()
    print(df.head(), "---")
    return df

def df_to_table(snf_conn,  df):
    table_name = settings.table_name.upper()
    print(df.head())
    success, num_chunks, num_rows, _= write_pandas(conn = snf_conn, df = df, table_name= table_name) 
    print( success, num_chunks, num_rows, _)
    return success

def update_scd1():
    with open("scd1.sql", "r") as file:
        file.read()


def update_scd2(cursor):
    with open("scd2.sql", "r") as file:
        query = file.read()
        cursor.execute(query)
        cursor.fetchall()

def main():
    # step 1 : read csv data
    df= read_csv_file(28)
    snf_conn = snowflake.connector.connect(
                user= settings.user,
                password= settings.password,
                account= settings.account,
                warehouse= settings.warehouse,
                database= settings.database,
                schema= settings.schema,
                role= settings.role # Optional
            )
        
    cursor = snf_conn.cursor()

    # Execute the query to get the current database and schema
    cursor.execute( "truncate table " + settings.schema+"."+settings.table_name)
    print(cursor.fetchall())
    if df_to_table(snf_conn,df):
        update_scd1()
        update_scd2(cursor)
        print("data loaded to table")
    else:
        print("error")

main()