import os , sys

# Get the current file's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the project directory
project_dir = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

# Append the project directory to sys.path
sys.path.append(project_dir)


from src.utils.schema_reader import SchemaReader
import numpy as np
import pandas as pd

class DatabasePreprocessor:
    def __init__(self, schema_path):
        self.schema_reader = SchemaReader(schema_path)
    
    def preprocess_df(self , df) :
        df = self._preprocess_integer_columns(df)
        return self._preprocess_text_cols(df)
        
    def _preprocess_integer_columns(self, df):
        """
        It replaces NaN values with None and converts the columns to the pd.Int64Dtype() data type.
        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            pandas.DataFrame: The preprocessed DataFrame.
        """
        integer_columns = self.schema_reader.get_integer_columns()

        for column in integer_columns:
            df[column] = df[column].replace(np.nan, None).astype(pd.Int64Dtype())

        return df
    
    def _preprocess_text_cols(self, df):
        """
        Adds quotes around the values of columns with VARCHAR data type in a DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame containing the data.

        Returns:
            pandas.DataFrame: The DataFrame with quotes added to VARCHAR columns.
        """
        schema = self.schema_reader.get_schema()

        for column, datatype in schema.items():
            if datatype.lower().startswith('varchar'):
                df[column] = df[column].apply(lambda x: f"'{x}'" if pd.notnull(x) else x)

        return df
