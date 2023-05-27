import pandas as pd
from sklearn.preprocessing import PolynomialFeatures, KBinsDiscretizer, StandardScaler

class FeatureEngineering:
    @staticmethod
    def normalize(X, columns):
        # Normalize the specified columns
        scaler = StandardScaler()
        X[columns] = scaler.fit_transform(X[columns])
        return X
    
    @staticmethod
    def add_age_flag(X):
        X['"AgeFlag"'] = 0
        X.loc[X['"age"'] >= 61, '"AgeFlag"'] = 1
        X.loc[X['"age"'] <= 18, '"AgeFlag"'] = 1
        return X
    
    @staticmethod
    def select_features(X, include_target=False):
        selected_features = ['"duration"', '"campaign"', '"pdays"', '"previous"', '"poutcomeFlag"', '"AgeFlag"']
        if include_target:
            selected_features.append('"y"')
        return X.loc[:, selected_features]
