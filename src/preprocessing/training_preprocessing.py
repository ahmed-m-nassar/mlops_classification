import os , sys

# Get the current file's directory
current_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the project directory
project_dir = os.path.abspath(os.path.join(current_dir, os.pardir, os.pardir))

# Append the project directory to sys.path
sys.path.append(project_dir)

from sklearn.base import BaseEstimator, TransformerMixin

class DataPreprocessor(BaseEstimator, TransformerMixin):
    def __init__(self, include_target ):
        self.include_target = include_target
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        X = self._map_target_values(X)
        X = self._add_age_flag(X)
        X = self._add_poutcome_flag(X)
        X = self._select_features(X)
        return X
    
    def _map_target_values(self, df):
        df['"y"'] = df['"y"'].map({'no': 0, 'yes': 1})
        return df
    
    def _add_age_flag(self, df):
        df['"AgeFlag"'] = 0
        df.loc[df['"age"'] >= 61, '"AgeFlag"'] = 1
        df.loc[df['"age"'] <= 18, '"AgeFlag"'] = 1
        return df
    
    def _add_poutcome_flag(self, df):
        df['"poutcomeFlag"'] = 0
        df.loc[df['"poutcomeFlag"'] == 'success', '"poutcomeFlag"'] = 1
        return df
    
    def _select_features(self, X):
        selected_features = ['"duration"', '"campaign"', '"pdays"', '"previous"', '"poutcomeFlag"', '"AgeFlag"']
        if self.include_target:
            selected_features.append('"y"')
        return X.loc[:, selected_features]