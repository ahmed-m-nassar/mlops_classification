class AddAgeFlag:
    def transform(self, X):
        X['"AgeFlag"'] = 0
        X.loc[X['"age"'] >= 61, '"AgeFlag"'] = 1
        X.loc[X['"age"'] <= 18, '"AgeFlag"'] = 1
        return X


class SelectFeatures:
    def __init__(self, include_target=False):
        self.include_target = include_target
    
    def transform(self, X):
        selected_features = ['"duration"', '"campaign"', '"pdays"', '"previous"', '"poutcomeFlag"', '"AgeFlag"']
        if self.include_target:
            selected_features.append('"y"')
        return X.loc[:, selected_features]
