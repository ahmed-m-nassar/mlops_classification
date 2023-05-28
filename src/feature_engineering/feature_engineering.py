class AddAgeFlag:
    def transform(self, X):
        X['"AgeFlag"'] = 0
        X.loc[X['"age"'] >= 61, '"AgeFlag"'] = 1
        X.loc[X['"age"'] <= 18, '"AgeFlag"'] = 1
        return X


class SelectFeatures:    
    def transform(self, X):
        selected_features = ['"duration"', '"campaign"', '"pdays"', '"previous"', '"poutcomeFlag"', '"AgeFlag"']
        if '"y"' in X.columns:
            selected_features.append('"y"')
        return X.loc[:, selected_features]
