import pandas as pd

csv_file_path = "E:\\projects\\mlops_classification\\mlops_classification\\data\\raw\\new_test.csv"

df = pd.read_csv(csv_file_path)

print(df.columns)