class MapTargetValues:
    def transform(self, X):
        X['"y"'] = X['"y"'].map({'no': 0, 'yes': 1})
        return X


class AddPoutcomeFlag:
    def transform(self, X):
        X['"poutcomeFlag"'] = 0
        X.loc[X['"poutcomeFlag"'] == 'success', '"poutcomeFlag"'] = 1
        return X