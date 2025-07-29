import pyodbc
from typing import Any, Literal, List, Dict, Tuple

def server_connection(server_name: str,
                      creds: list,
                      database_name:str = 'master',
                      driver:str = '{ODBC Driver 17 for SQL Server}',
                      autocommit=False):
    """
    Creates a new SQL Server database with the specified name if it doesn't already exist.

    Parameters:
        database_name (str): The name of the database to be created.
        cursor: A database cursor object used to execute SQL statements.
        connection: A database connection object used to commit changes and retrieve server information.

    Behavior:
        - Checks if a database with the given name exists.
        - If not, creates the database and commits the transaction.
        - If the database exists, prints a message including the server name.
        - Closes both the cursor and connection after operation.

    Returns:
        Exception: If an error occurs during execution, the exception is returned.
    """
    try:
        if len(creds) != 2:
            return TypeError("Credentials list with 2 elements: [username, password]")

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
    """
    Creates a new SQL Server database with the specified name if it doesn't already exist.

    Parameters:
        database_name (str): The name of the database to be created.
        cursor: A database cursor object used to execute SQL statements.
        connection: A database connection object used to commit changes and retrieve server information.

    Behavior:
        - Checks if a database with the given name exists.
        - If not, creates the database and commits the transaction.
        - If the database exists, prints a message including the server name.
        - Closes both the cursor and connection after operation.

    Returns:
        Exception: If an error occurs during execution, the exception is returned.
    """
    try:
        database_present = cursor.execute(f"SELECT name FROM sys.databases WHERE name = '{database_name}'").fetchall()
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


def create_new_table(table_name: str, cursor, connection):

    return 1
def generate_template_table(table_name: str, cursor, connection):
    try:
        table_present = cursor.execute(f""" SELECT TABLE_NAME
                                            FROM INFORMATION_SCHEMA.TABLES
                                            WHERE TABLE_TYPE='BASE TABLE'
                                            AND TABLE_NAME='{table_name}'""").fetchall()
        if table_present == []:
            cursor.execute(f'''
            CREATE TABLE {table_name} (
                    PersonId INTEGER PRIMARY KEY,
                    FirstName NVARCHAR NOT NULL,
                    LastName  NVARCHAR NOT NULL,
                    Age INTEGER NULL,
                    CreatedAt DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
                    );
                    ''')
            connection.commit()
            cursor.close()
            connection.close()
        else:
            print(f"There is already a table with the name: {table_name} " + 
                    f"in {connection.getinfo(pyodbc.SQL_SERVER_NAME)}")
            cursor.close()
            connection.close()

    except Exception as e:
        return e



def generate_schema_from_df(dataframe):
    pass

def generate_table_from_schema(schema:str ):
    pass
