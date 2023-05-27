import pandas as pd
from sklearn.preprocessing import OneHotEncoder, LabelEncoder

class Encoding:
    
    @staticmethod
    def add_poutcome_flag(X):
        X['"poutcomeFlag"'] = 0
        X.loc[X['"poutcomeFlag"'] == 'success', '"poutcomeFlag"'] = 1
        return X
    
    @staticmethod
    def map_target_values(X):
        X['"y"'] = X['"y"'].map({'no': 0, 'yes': 1})
        return X