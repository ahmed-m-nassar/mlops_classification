import os , sys
current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sys.path.append(project_dir)

from database.manager import DatabaseManager
from data_cleaning.clean_data import DataCleaning
from encoding.encoding import Encoding
from feature_engineering.feature_engineering import FeatureEngineering


import yaml
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
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
    
    train_table_name = params['database']['train_table_name']
    
    df = db_manager.select_from_table(table_name=train_table_name ,
                             schema_file_path=params['schemas']['training_schema_path'])


    #splitting dataframe
    ############################################################
    # Split the dataset into train and validation sets
    train_df, val_df = train_test_split(df, test_size=0.2, random_state=42 )
    #############################################################
    
    # Process training dataset
    ##########################################################################

    # Instantiate the DataPreprocessor class
    preprocessor = DataPreprocessor(include_target= True)

    # Define the preprocessing steps for training data
    training_pipeline = Pipeline([
        ('preprocessor', preprocessor) ,
        
    ])

    # Apply the pipelines to your data
    training_df = training_pipeline.fit_transform(df)
    #############################################################################     
    