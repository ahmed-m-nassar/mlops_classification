import os , sys
# Get the current file's directory
current_dir = os.path.dirname(os.path.abspath(__file__))
# Construct the absolute path to the project directory
project_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
# Append the project directory to sys.path
sys.path.append(project_dir)

import yaml
import pandas as pd
import logging
from database.manager import DatabaseManager
from preprocessing.database_preprocessing import DatabasePreprocessor
from utils.schema_reader import SchemaReader


# Configure logging
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


def create_table(db_manager, schema_reader, table_name):
    """
    Creates a table in the database.

    Args:
        db_manager (DatabaseManager): An instance of DatabaseManager for database operations.
        schema_reader (SchemaReader): An instance of SchemaReader to read the schema.
        table_name (str): The name of the table to be created.

    Returns:
        None
    """
    col_datatype = schema_reader.get_col_name_data_type_str()
    db_manager.create_table(table_name=table_name, schema=col_datatype)


def read_dataset(file_path):
    """
    Reads a dataset from a CSV file.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pandas.DataFrame: The dataset as a DataFrame.
    """
    dataset = pd.read_csv(file_path)
    return dataset


def insert_data(db_manager, schema_reader, table_name, dataset):
    """
    Inserts the data into the database.

    Args:
        db_manager (DatabaseManager): An instance of DatabaseManager for database operations.
        schema_reader (SchemaReader): An instance of SchemaReader to get column names.
        table_name (str): The name of the table to insert data into.
        dataset (pandas.DataFrame): The dataset as a DataFrame.

    Returns:
        None
    """
    database_preprocessor = DatabasePreprocessor(schema_path=schema_reader.get_schema_path())
    dataset = database_preprocessor.preprocess_df(dataset)
    records = dataset.to_records(index=False)
    values = [tuple(record) for record in records]

    db_manager.insert_into_table(column_names=schema_reader.get_column_names(),
                                 table_name=table_name,
                                 values=values)


if __name__ == "__main__":
    # Read configuration parameters
    params = read_params_file(os.path.join('config', 'params.yaml'))

    # Connect to the database
    db_manager = connect_to_database(params)

    # Process training dataset
    ########################################################################
    train_table_name = params['database']['train_table_name']
    train_schema_path = params['schemas']['training_schema_path']
    train_dataset_path = params['load_data']['training_raw_dataset_csv']

    schema_reader = SchemaReader(train_schema_path)
    create_table(db_manager, schema_reader, train_table_name)
    training_dataset = read_dataset(train_dataset_path)
    insert_data(db_manager, schema_reader, train_table_name, training_dataset)
    #########################################################################
    
    # Process test dataset
    #########################################################################
    test_table_name = params['database']['test_table_name']
    test_schema_path = params['schemas']['prediction_schema_path']
    test_dataset_path = params['load_data']['testing_raw_dataset_csv']

    schema_reader = SchemaReader(test_schema_path)
    create_table(db_manager, schema_reader, test_table_name)
    test_dataset = read_dataset(test_dataset_path)
    insert_data(db_manager, schema_reader, test_table_name, test_dataset)
    ##########################################################################