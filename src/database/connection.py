"""
connection.py

This module contains the Connection class for establishing and managing a database connection.

Classes:
    Connection: A class for handling database connection, execution of queries, and disconnection.

"""

import psycopg2

class Connection:
    """
    Connection class handles the database connection, execution of queries, and disconnection.

    Attributes:
        host (str): The hostname or IP address of the database server.
        dbname (str): The name of the database.
        user (str): The username to connect to the database.
        password (str): The password for the database user.
        port (int): The port number on which the database server is listening.
        connection (psycopg2.extensions.connection): The database connection object.
        cursor (psycopg2.extensions.cursor): The database cursor object.

    Methods:
        connect: Establishes a connection to the database.
        execute_query: Executes the provided query on the connected database.
        disconnect: Closes the database connection.
    """

    def __init__(self, host, dbname, user, password, port):
        """
        Initializes the Connection object.

        Args:
            host (str): The hostname or IP address of the database server.
            dbname (str): The name of the database.
            user (str): The username to connect to the database.
            password (str): The password for the database user.
            port (int): The port number on which the database server is listening.
        """
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Establishes a connection to the database.
        """
        self.connection = psycopg2.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            port=self.port
        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        """
        Executes the provided query on the connected database.

        Args:
            query (str): The SQL query to execute.
        """
        self.cursor.execute(query)
        self.connection.commit()

    def disconnect(self):
        """
        Closes the database connection.
        """
        self.cursor.close()
        self.connection.close()
        self.connection = None
        self.cursor = None
