# importing the needed libraries.
from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

# It's a good approach to make the username & password anonymous.
# Import the username & password as enviroment variables
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']

# can use the server IP if needed.
server = "localhost"  

def load(df, tbl):
    try:
        row_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/bikestores')
        print(f'Importing rows {row_imported} to {row_imported + len(df)}... for table {tbl}')
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        row_imported += len(df)
        print("Data imported successfully")
    except Exception as e:
        print("Data load error: " + str(e))

def extract():
    try:
        # Connecting To MSSQL Server with the parameters specified (Driver - ServerName - The wanted Database To be Connected - Windows Authentication to Connect).
        # You can know the Server Name by excuting the following query on MSSQL server
        # SELECT @@ServerName   
        connection = pyodbc.connect('DRIVER={SQL Server};'
                                    'Server=DESKTOP-KNNT8US\SQLEXPRESS;'
                                    'Database=BikeStores;'
                                    'Trusted_Connection=True')
        # Excuted SQL queried must remain in a stable point in the database which is the commit point.
        # So we use the cursor method for the sql quries.
        # Using the Excute Function to excute the query.
        cursor = connection.cursor()
        # Here in this query the needed tables names get fetched from the sys.tables
        cursor.execute("""
            SELECT s.name AS schema_name, t.name AS table_name
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE t.name IN ('brands', 'categories', 'products', 'stocks', 
                             'customers', 'order_items', 'orders', 'staffs', 'stores')
              AND s.name = 'production'
        """)
        # Using fetchall method to return a list of the tables names
        tables = cursor.fetchall()
        for schema_name, table_name in tables:
            full_table_name = f"{schema_name}.{table_name}"
            # converting the table into a dataFrame
            df = pd.read_sql_query(f'SELECT * FROM {full_table_name}', connection)
            # using the previously created function load to load the dataFrame into the created tables in postgres.
            load(df, table_name)
        print('Data extraction completed successfully')
    # Handling the error that might be because of wrong imported parameters
    except pyodbc.Error as ex:
        print(f"Connection failed: {ex}")
    except Exception as e:
        print(f"Error while extracting data: {e}")
    finally:
        connection.close()

try:
    extract()
except Exception as e:
    print("Error while running extract function: " + str(e))
