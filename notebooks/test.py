import psycopg2
import pandas as pd
import numpy as np

conn = psycopg2.connect(host = 'localhost' , dbname = 'postgres' , user  = 'postgres' ,
                        password = 'admin' , port = 5432)

cur = conn.cursor()

# Specify the table name and column definitions
table_name = "customermaster"


columns = [
    "age INTEGER",
    "job VARCHAR(255)",
    "marital VARCHAR(255)",
    "education VARCHAR(255)",
    "\"default\" VARCHAR(255)",
    "housing VARCHAR(255)",
    "loan VARCHAR(255)",
    "contact VARCHAR(255)",
    "month VARCHAR(255)",
    "day_of_week VARCHAR(255)",
    "duration INTEGER",
    "campaign INTEGER",
    "pdays INTEGER",
    "previous INTEGER",
    "poutcome VARCHAR(255)",
    "y VARCHAR(255)"
]


# Create the table
create_table_query = f"CREATE TABLE {table_name} ({', '.join(columns)})"
cur.execute(create_table_query)

# Specify the CSV file path and table name
csv_file_path = "E:\\projects\\mlops_classification\\mlops_classification\\data\\raw\\new_train.csv"
table_name = "CustomerMaster"

# Read the CSV file using pandas
dataframe = pd.read_csv(csv_file_path  )
# Print the column names from the DataFrame
print(dataframe.columns)
# Convert specific columns to appropriate data types
integer_columns = ['age', 'duration', 'campaign', 'pdays', 'previous']
for column in integer_columns:
    dataframe[column] = dataframe[column].replace(np.nan, None).astype(pd.Int64Dtype())

# Convert the dataframe to a list of tuples
records = dataframe.to_records(index=False)
values = [tuple(record) for record in records]

# Create the INSERT query
column_names =  ['age', 'job', 'marital', 'education', '"default"', 'housing', 'loan', 'contact', 'month', 'day_of_week', 'duration', 'campaign', 'pdays', 'previous', 'poutcome', 'y']
placeholders = ', '.join(['%s'] * len(column_names))
column_names =  ', '.join(column_names)
insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

# Execute the INSERT query
cur.executemany(insert_query, values)



conn.commit()

cur.close()