import psycopg2
import pandas as pd
import numpy as np
import os
import yaml
import argparse
import json


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
    
def generate_create_table_query (table_name, schema):
    """
    Generates a CREATE TABLE query based on the provided table name and schema.

    Args:
        table_name (str): The name of the table to be created.
        schema (str): The path to a JSON schema file that defines the table schema.

    Returns:
        str: The CREATE TABLE query.
    """
    query = f"CREATE TABLE {table_name} ("
    
    with open(schema, 'r') as file:
        schema = json.load(file)
        
    for column, datatype in schema.items():
        query += f'"{column}" {datatype}, '
    
    query = query[:-2] + ")"
    
    return query

def generate_insert_into_table_query(table_name , schema_path):
    # Create the INSERT query
    column_names =  read_column_names_from_schema(schema_path=schema_path)
    placeholders = ', '.join(['%s'] * len(column_names))
    column_names =  ', '.join(column_names)
    insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
    return insert_query

def read_column_names_from_schema(schema_path) :
    """
    Extracts column names from a JSON file and returns them as a list with double quotes around each name.

    Args:
        json_file (str): The path to the JSON file containing the column names.

    Returns:
        list: A list of column names with double quotes around each name.
    """
    with open(schema_path, 'r') as file:
        schema_data = json.load(file)

    column_names = ['"' + name + '"' for name in schema_data.keys()]
    return column_names
    
def preprocess_columns(df , schema_path):
    with open(schema_path, 'r') as file:
        schema_data = json.load(file)
    integer_columns = [column for column, datatype in schema_data.items() if datatype == "INTEGER"]
    
    for column in integer_columns:
        df[column] = df[column].replace(np.nan, None).astype(pd.Int64Dtype())
    return df

if __name__ == "__main__" :
    params = read_params_file(os.path.join('config' , 'params.yaml'))
    database_connection = connect_to_database(host = params['database']['config']['host'] , 
                                              dbname =  params['database']['config']['dbname'] ,
                                              user = params['database']['config']['user'] ,
                                              password =  params['database']['config']['password'] ,
                                              port =  params['database']['config']['port'] )  
    
    database_cursor = database_connection.cursor() 
    
    #Creating training data query
    create_training_table_query = generate_create_table_query(params['database']['train_table_name'] ,
                                      params['schemas']['training_schema_path'])
    #executing the query
    database_cursor.execute(create_training_table_query)
    
    #loading training dataset
    training_df = pd.read_csv(params['load_data']['training_raw_dataset_csv'])
    #Preprocessing training dataset
    training_df = preprocess_columns(training_df ,
                       params['schemas']['training_schema_path'])
    
    #generating insert query
    insert_query = generate_insert_into_table_query(params['database']['train_table_name'],
                                                    params['schemas']['training_schema_path'])
    
    records = training_df.to_records(index=False)
    values = [tuple(record) for record in records]
    database_cursor.executemany(insert_query, values)
    
    database_connection.commit()

    database_cursor.close()