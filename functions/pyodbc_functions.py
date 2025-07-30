import pyodbc
from typing import Any, Literal, List, Dict, Tuple
import decimal
from datetime import datetime, date, time
import pandas as pd

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



def generate_schema_from_df(dataframe: pd.DataFrame, limit_string_search:int=0) -> str:
    """
    Generates a SQL table schema from a pandas DataFrame by mapping column data types
    to SQL-compatible types.

    Parameters:
        dataframe (pandas.DataFrame): The input DataFrame whose columns will be analyzed for type inference.
        limit_string_search ((int), optional (default=0)): If greater than zero, only the first N rows will be 
        used to assess the maximum string length for VARCHAR vs TEXT inference. Reduces processing time on 
        large datasets.

    Returns:
        schema (str): A formatted SQL string representing the CREATE TABLE schema with column names and 
        inferred data types.
    """
    type_mapping = {
        'int64': 'BIGINT',
        'int32': 'INT',
        'int16': 'SMALLINT',
        'float64': 'FLOAT',
        'float32': 'REAL',
        'bool': 'BIT',
        'object': 'TEXT',
        'datetime64[ns]': 'TIMESTAMP' }
    schema = ""
    column_definitions = []
    for col in dataframe.columns:
        data_type = dataframe[col].dtype

        if data_type == 'object': # Treat objects separately
            if not dataframe[col].dropna().empty:
                data_sample = dataframe[col].dropna().iloc[0]

                if isinstance(data_sample, bytes) or isinstance(data_sample, bytearray):
                    sql_type = 'VARBINARY(MAX)'

                elif isinstance(data_sample, str):
                    if limit_string_search > 0:
                        data = dataframe[col].iloc[:limit_string_search]
                    else:
                        data = dataframe[col]
                    if max(len(str(value)) for value in data) < 255:
                        sql_type = 'VARCHAR(255)'
                    else:
                        sql_type = 'TEXT'

                elif isinstance(data_sample, decimal.Decimal):
                    sign, digits, exponent = data_sample.as_tuple()
                    scale = -int(exponent) if int(exponent) < 0 else 0

                    if set(str(data_sample).split('.')[0].lstrip('-')) == set('0'):
                        # Only zeros before decimal point
                        precision = len(str(data_sample).split('.')[1])
                    else:
                        # Non-zeros before decimal point
                        precision = len(str(data_sample).lstrip('-').replace('.',''))

                    sql_type = f'DECIMAL({precision},{scale})'

                elif isinstance(data_sample, datetime):
                    sql_type = 'TIMESTAMP'
                else:
                    sql_type = 'TEXT' # Assign when the datatype is not one of the written options
            else: 
                sql_type = 'TEXT' # Assign when the dataframe column is empty
        else: 
            sql_type = type_mapping.get(str(data_type), 'TEXT')

        column_definitions.append(f" {col} {sql_type}")
        
    schema += ",\n".join(column_definitions)

    return schema



def create_table_from_schema(schema:str, table_name:str, cursor, connection):
    """
    Creates a SQL table from a schema in text form.

    Parameters
    ----------
        schema : str
            The schema of the database in string form. 
        table_name : str 
            The name of the database to be created.
        cursor : pyodbc.cursor
            A database cursor object used to execute SQL statements.
        connection : pyodbc.Connection 
            A database connection object used to commit changes and retrieve server information.

    Returns
    -------
        str or None
            Returns a string describing the error if one occurs during execution or if the table already exists, otherwise returns None after successful table creation.
    """
    try:
        table_present = cursor.execute(f""" SELECT TABLE_NAME
                                            FROM INFORMATION_SCHEMA.TABLES
                                            WHERE TABLE_TYPE='BASE TABLE'
                                            AND TABLE_NAME='{table_name}'""").fetchall()
        if table_present == []: # No table with that name
            cursor.execute(f'''
                CREATE TABLE {table_name} (
                        {schema}
                        );
                        ''')
            connection.commit()
            cursor.close()
            connection.close()
        else:
            already_table_message = (f"There is already a table with the name: {table_name}") + \
                                     (f" in {connection.getinfo(pyodbc.SQL_SERVER_NAME)}")
            cursor.close()
            connection.close()
            return already_table_message

    except Exception as e:
        return f'There was an error: {e}'



def return_table_schema(table_name: str, cursor: pyodbc.Cursor, connection: pyodbc.Connection):
    """
    Generates and returns the SQL column definitions for a specified table in a pyodbc-supported database.

    Parameters
    ----------
    table_name : str
        The name of the table whose schema will be retrieved.
    cursor : pyodbc.Cursor
        An active pyodbc cursor used to query the database metadata.
    connection : pyodbc.Connection
        An open pyodbc connection object associated with the cursor.

    Returns
    -------
    str
        A formatted string representing the column definitions of the table,
        including column names, data types, and any size/precision attributes.
    """
    schema = ""
    schema_lines = []
    try:
        for col in cursor.columns(table=table_name):
            #print(col.column_name, (col.type_name).upper(), col.column_size, col.decimal_digits)

            if col.type_name.upper() == 'VARCHAR':
                schema_lines.append(f" {col.column_name} {(col.type_name).upper()}({col.column_size})")

            elif col.type_name.upper() == 'DECIMAL':
                schema_lines.append(f" {col.column_name} {(col.type_name).upper()}({col.column_size},{col.decimal_digits})")
            
            elif col.type_name.upper() == 'VARBINARY':
                if col.column_size == 0:
                    col_size = 'MAX'
                    schema_lines.append(f" {col.column_name} {(col.type_name).upper()}({col_size})")
                else:
                    schema_lines.append(f" {col.column_name} {(col.type_name).upper()}({col.column_size})")

            else:
                schema_lines.append(f" {col.column_name} {(col.type_name).upper()}")
        
        schema += ",\n".join(schema_lines)
        cursor.close()
        connection.close()
        return schema

    except Exception as e:
        return f'There was an error: {e}'
