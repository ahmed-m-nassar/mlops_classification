from utils.schema_reader import SchemaReader
import numpy as np
import pandas as pd

class DatabasePreprocessor:
    def __init__(self, schema_path):
        self.schema_reader = SchemaReader(schema_path)

    def preprocess_columns(self, df):
        integer_columns = self.schema_reader.get_integer_columns()

        for column in integer_columns:
            df[column] = df[column].replace(np.nan, None).astype(pd.Int64Dtype())

        return df
