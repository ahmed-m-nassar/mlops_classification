"""
manager.py

This module contains the DatabaseManager class for managing database operations.

Classes:
    DatabaseManager: A class for managing database operations such as creating tables and inserting data.

"""
import os , sys

# Get the current file's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the project directory
project_dir = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

# Append the project directory to sys.path
sys.path.append(project_dir)


from src.database.connection import Connection
from src.utils.schema_reader import SchemaReader
import pandas as pd

class DatabaseManager:
    """
    DatabaseManager class handles various database operations such as creating tables and inserting data.

    Attributes:
        connection (Connection): An instance of the Connection class to establish and manage database connections.

    Methods:
        create_table: Creates a table in the database.
        insert_into_table: Inserts data into a table in the database.
    """

    def __init__(self, host, dbname, user, password, port):
        """
        Initializes the DatabaseManager object.

        Args:
            host (str): The hostname or IP address of the database server.
            dbname (str): The name of the database.
            user (str): The username to connect to the database.
            password (str): The password for the database user.
            port (int): The port number on which the database server is listening.
        """
        self.connection = Connection(host, dbname, user, password, port)

    def create_table(self, table_name, schema):
        """
        Creates a table in the database.

        Args:
            table_name (str): The name of the table to be created.
            schema (str): The schema definition for the table.

        Returns:
            None
        """
        query = f"CREATE TABLE {table_name} ({schema})"
        self.connection.connect()
        self.connection.execute_query(query)
        self.connection.disconnect()

    def insert_into_table(self, table_name, column_names, values):
        """
        Inserts data into a table in the database.

        Args:
            table_name (str): The name of the table to insert data into.
            column_names (list): A list of column names specifying the columns to insert data into.
            values (list): A list of tuples containing the data to be inserted.

        Returns:
            None
        """
        placeholders = ', '.join(['%s'] * len(column_names))
        column_names = ', '.join(column_names)
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        self.connection.connect()
        for value in values:
            self.connection.execute_query(query % tuple(value))
        self.connection.disconnect()
        
    def select_from_table(self, table_name, schema_file_path, column_names=None):
        """
        Executes a SELECT query on a table in the database.

        Args:
            table_name (str): The name of the table to select from.
            schema_file_path (str): The file path to the database schema file.
            column_names (list, optional): A list of column names to select (default is None).

        Returns:
            pd.DataFrame: A Pandas DataFrame containing the result of the query.
        """
        if column_names is None:
            query = f"SELECT * FROM {table_name}"
        else:
            columns = ", ".join(column_names)
            query = f"SELECT {columns} FROM {table_name}"

        self.connection.connect()
        result = self.connection.execute_query(query, fetch_result=True)
        self.connection.disconnect()

        if result:
            # Retrieve column names from the database schema
            if column_names is None:
                schema_reader = SchemaReader(schema_file_path)
                column_names = schema_reader.get_column_names()

            df = pd.DataFrame(result, columns=column_names)
            return df
        else:
            return pd.DataFrame()
