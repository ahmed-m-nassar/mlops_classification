import psycopg2
import pandas as pd
import numpy as np
import os
import yaml
import argparse


def read_params_file(file_path):
    """
    Reads a YAML parameters file and returns the content as a dictionary.

    Args:
        file_path (str): The path to the YAML parameters file.

    Returns:
        dict: A dictionary containing the parameters read from the file.    
    """
    with open(file_path) as yaml_file:
        params = yaml.safe_load(yaml_file)
    return params
    
    
def connect_to_database(host , dbname , user , password , port) :
    """
    Establishes a connection to a PostgreSQL database and returns the connection object.

    Args:
        host (str): The hostname or IP address of the database server.
        dbname (str): The name of the database.
        user (str): The username to connect to the database.
        password (str): The password for the database user.
        port (int): The port number on which the database server is listening.

    Returns:
        psycopg2.extensions.connection: A connection object representing the connection to the database.
    """
    return  psycopg2.connect(host =  host, dbname = dbname , user  = user ,
                        password = password , port = port)
    
def create_table_query(table_name, schema):
    """
    Constructs a CREATE TABLE query based on the provided table name and schema.

    Args:
        table_name (str): The name of the table to be created.
        schema (dict): A dictionary representing the table schema. The keys are column names,
                       and the values are the corresponding data types.

    Returns:
        str: The CREATE TABLE query.
    """
    query = f"CREATE TABLE {table_name} ("
    
    for column, datatype in schema.items():
        query += f"{column} {datatype}, "
    
    query = query[:-2] + ")"
    
    return query


    
if __name__ == "__main__" :
    params = read_params_file(os.path.join('config' , 'params.yaml'))
    database_connection = connect_to_database(host = params['database']['config']['host'] , 
                                              dbname =  params['database']['config']['dbname'] ,
                                              user = params['database']['config']['user'] ,
                                              password =  params['database']['config']['password'] ,
                                              port =  params['database']['config']['port'] )  
    
    database_cursor = database_connection.cursor() 
    
    
    print(database_connection) 