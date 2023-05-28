import os , sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(project_dir)

import yaml
import pandas as pd
from database.manager import DatabaseManager
import pickle

import logging
logging.basicConfig(level=logging.INFO)

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


def connect_to_database(params):
    """
    Connects to the database using the provided parameters.

    Args:
        params (dict): Database connection parameters.

    Returns:
        DatabaseManager: An instance of DatabaseManager representing the database connection.
    """
    db_manager = DatabaseManager(dbname=params['database']['config']['dbname'],
                                 host=params['database']['config']['host'],
                                 user=params['database']['config']['user'],
                                 port=params['database']['config']['port'],
                                 password=params['database']['config']['password'])
    return db_manager

if __name__ == "__main__":
    params = read_params_file(os.path.join('config', 'params.yaml'))

    # Connect to the database
    db_manager = connect_to_database(params)