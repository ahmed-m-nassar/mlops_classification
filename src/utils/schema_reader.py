import json

class SchemaReader:
    """
    Utility class for reading and processing table schemas from JSON files.
    """

    def __init__(self, schema_path):
        self.schema_path = schema_path
        self.schema = self.read_schema()

    def _read_schema(self):
        """
        Reads a table schema from a JSON file.

        Returns:
            dict: A dictionary representing the table schema.
        """
        with open(self.schema_path, 'r') as file:
            schema_data = json.load(file)

        return schema_data

    def get_column_names(self):
        """
        Returns the column names from the table schema.

        Returns:
            list: A list of column names.
        """
        return list(self.schema.keys())

    def get_data_types(self):
        """
        Returns the data types from the table schema.

        Returns:
            dict: A dictionary mapping column names to data types.
        """
        return self.schema
    
    def get_integer_columns(self):
        """
        Returns the INTEGER datatypes from the table schema.

        Returns:
            list: a list of names of INTEGER columns.
        """
        integer_columns = [column for column, datatype in self.schema_data.items() if datatype == 'INTEGER']
        return integer_columns

