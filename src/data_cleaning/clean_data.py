class HandleMissingValues:
    def __init__(self, strategy='mean'):
        self.strategy = strategy

    def transform(self, X):
        if self.strategy == 'mean':
            X.fillna(X.mean(), inplace=True)
        elif self.strategy == 'median':
            X.fillna(X.median(), inplace=True)
        elif self.strategy == 'mode':
            X.fillna(X.mode().iloc[0], inplace=True)
        else:
            raise ValueError(f"Invalid strategy: {self.strategy}")
        return X

class RemoveOutliers:
    def __init__(self, method='iqr', multiplier=1.5):
        self.method = method
        self.multiplier = multiplier

    def transform(self, X):
        if self.method == 'iqr':
            Q1 = X.quantile(0.25)
            Q3 = X.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - self.multiplier * IQR
            upper_bound = Q3 + self.multiplier * IQR
            X = X[(X >= lower_bound) & (X <= upper_bound)]
        elif self.method == 'z-score':
            z_scores = (X - X.mean()) / X.std()
            X = X[abs(z_scores) <= self.multiplier]
        else:
            raise ValueError(f"Invalid method: {self.method}")
        return X
