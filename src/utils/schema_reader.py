import json

class SchemaReader:
    """
    Utility class for reading and processing table schemas from JSON files.
    """

    def __init__(self, schema_path):
        self.schema_path = schema_path
        self.schema = self._read_schema()

    def _read_schema(self):
        """
        Reads a table schema from a JSON file.

        Returns:
            dict: A dictionary representing the table schema.
        """
        with open(self.schema_path, 'r') as file:
            schema_data = json.load(file)

        return schema_data

    def get_schema_path(self):
        return self.schema_path
        
    def get_column_names(self):
        """
        Returns the column names from the table schema.

        Returns:
            list: A list of column names.
        """
        return ['"' + name + '"' for name in self.schema.keys()]

    def get_schema(self):
        """
        Returns the schema.

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
        integer_columns = [column for column, datatype in self.schema.items() if datatype == 'INTEGER']
        return integer_columns

    def get_col_name_data_type_str(self) :
        col_datatype = ""
        for column, datatype in self.schema.items():
            col_datatype += f'"{column}" {datatype}, '
        
        col_datatype = col_datatype[:-2] 
        return col_datatype