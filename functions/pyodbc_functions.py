import pyodbc
from typing import Any, Literal, List, Dict, Tuple

def server_connection(server_name: str,
                      creds: list,
                      database_name:str = 'master',
                      driver:str = '{ODBC Driver 17 for SQL Server}',
                      autocommit=False):
    try:
        if len(creds) != 2:
            return TypeError("Credentials list must be of length 2")

        connection = pyodbc.connect(
                                     f"DRIVER={driver};"
                                     f"SERVER={server_name};"
                                     f"DATABASE={database_name};"
                                     f"UID={creds[0]};"
                                     f"PWD={creds[1]};",
                                     autocommit=autocommit
        )
        cursor = connection.cursor()
        print(f"Successfully connected to server: {connection.getinfo(pyodbc.SQL_SERVER_NAME)}")
        print(f"Connected to database: {cursor.execute("SELECT DB_NAME()").fetchall()[0][0]}")
    except Exception as e:
        return e

    return connection, cursor



def create_new_database(database_name: str, cursor, connection):
    try:
        database_present = cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{database_name}'")
        if database_present == []:
            cursor.execute(f"CREATE DATABASE [{database_name}]")
            connection.commit()
            cursor.close()
            connection.close()
        else:
            print(f"There is already a database with the name: {database_name} " + 
                  f"in {connection.getinfo(pyodbc.SQL_SERVER_NAME)}")
            cursor.close()
            connection.close()
    
    except Exception as e:
        return e

