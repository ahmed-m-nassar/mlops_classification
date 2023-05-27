import pandas as pd
import numpy as np

class DataCleaning:
    @staticmethod
    def drop_missing_values(X):
        # Drop rows with missing values
        X = X.dropna()
        return X
    
    @staticmethod
    def handle_outliers_iqr(X, columns, threshold=1.5):
        for col in columns:
            q1 = np.percentile(X[col], 25)
            q3 = np.percentile(X[col], 75)
            iqr = q3 - q1
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr
            X.loc[X[col] < lower_bound, col] = lower_bound
            X.loc[X[col] > upper_bound, col] = upper_bound
        return X